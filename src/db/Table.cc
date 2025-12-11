#include "db/Table.h"

#include <cstdint>
#include <iomanip>

#include "common/Type.h"
#include "db/Format.h"
#include "db/ValueType.h"
#include "util/Logger.h"

bool DbTable::InsertRecordLocal(RecordKey key, const DbRecord &row, version_t v) {
    assert(TableLocalPtr() != nullptr);

    char *bkt = LocateBucketLocal(key);
    for (SlotId s_id = 0; s_id < primary_index_.RecordsPerBucket(); ++s_id) {
        RecordHeader *record_header = GetRecord(bkt, s_id);
        if (record_header->InUse() && record_header->record_key != key) {
            continue;
        }

        // Fill in Record Header with corresponding metadata
        std::memset(record_header, 0, RecordHeaderSize);
        record_header->record_key = key;
        record_header->lock = 0;
        record_header->SetInuse();

        // Fill in each column:
        for (int cid = 0; cid < txn_schema_.GetColumnCount(); ++cid) {
            ColumnHeader *c = GetColumn(record_header, cid);
            c->version = v;
            row.CopyTo(cid, c->ColumnData());
            // Set the subversion of the corresponding column
            record_header->UpdateSubVersion(SubVersion(v), cid);
        }

        // Set the last signal byte:
        char *signal_byte = (char *)record_header;
        signal_byte[txn_schema_.GetRecordMemSize() - 1] = VISIBLE_DATA;
        return true;
    }
    return false;
}

bool DbTable::UpdateRecordLocal(RecordKey key, const DbRecord &record, version_t v) {
    assert(TableLocalPtr() != nullptr);
    char *bkt = LocateBucketLocal(key);
    for (int r_id = 0; r_id < primary_index_.RecordsPerBucket(); ++r_id) {
        RecordHeader *record_header = GetRecord(bkt, r_id);
        if (record_header->RecordKeyMatch(key)) {
            // Find a matched record key, update the entire record
            for (int cid = 0; cid < txn_schema_.GetColumnCount(); ++cid) {
                ColumnHeader *c = GetColumn(record_header, cid);
                c->version = v;
                record.CopyTo(cid, c->ColumnData());
                record_header->UpdateSubVersion(SubVersion(v), cid);
            }
            return true;
        }
    }
    return false;
}

bool DbTable::SearchRecordLocal(RecordKey key, DbRecord *record) {
    assert(TableLocalPtr() != nullptr);
    char *bkt = LocateBucketLocal(key);
    for (int r_id = 0; r_id < primary_index_.RecordsPerBucket(); ++r_id) {
        RecordHeader *record_header = GetRecord(bkt, r_id);
        if (record_header->RecordKeyMatch(key)) {
            for (int cid = 0; cid < txn_schema_.GetColumnCount(); ++cid) {
                ColumnHeader *c = GetColumn(record_header, cid);
                record->CopyFrom(cid, c->ColumnData());
                // This is a read operation: no need to update the subversion
            }
            return true;
        }
    }
    return false;
}

bool DbTable::DeleteRecordLocal(RecordKey key, DbRecord *record) {
    assert(TableLocalPtr() != nullptr);
    char *bkt = LocateBucketLocal(key);
    for (SlotId s_id = 0; s_id < primary_index_.RecordsPerBucket(); ++s_id) {
        RecordHeader *record_header = GetRecord(bkt, s_id);
        if (record_header->RecordKeyMatch(key)) {
            for (int cid = 0; cid < txn_schema_.GetColumnCount(); ++cid) {
                ColumnHeader *c = GetColumn(record_header, cid);
                record->CopyTo(cid, c->ColumnData());
            }
            // Mark this record as unused
            record_header->SetUnuse();
            return true;
        }
    }
    return false;
}

char *DbTable::SerializeTableInfo(char *dst) const {
    // Write down the table's pool address for CN to contact
    *(TableAccessAttr *)dst = access_attr_;
    dst += sizeof(TableAccessAttr);
    // Write down the slot cache for the CN to know which slots are empty
    // dst = slot_cache_.Serialize(dst);
    return dst;
}

const char *DbTable::DeserializeTableInfo(const char *src) {
    // CN's table context reads the data
    TableAccessAttr attr = *(TableAccessAttr *)src;
    src += sizeof(TableAccessAttr);

    if (attr.table_type == TableType::PRIMARY) {
        this->SetPrimaryTablePoolPtr(attr.pool_addr);
        this->access_attr_.pool_addr = attr.pool_addr;
        LOG_DEBUG("Get Primary Table%u pool address%#llx", GetTableId(), access_attr_.pool_addr);
    } else {
        this->AddBackupTablePoolPtr(attr.pool_addr);
        LOG_DEBUG("Get Backup Table%u pool address%#llx", GetTableId(), attr.pool_addr);
    }

    LOG_DEBUG("Get Table%u pool address%#llx", GetTableId(), access_attr_.pool_addr);
    return src;
}

std::string MakeCenterString(size_t sz, const std::string &s) {
    if (sz < s.size()) {
        return s.substr(0, sz);
    }
    size_t padd_sz = (sz - s.size()) / 2;
    return std::string(padd_sz, ' ') + s + std::string(sz - s.size() - padd_sz, ' ');
}

template <typename T>
std::string ToHexString(T a) {
    std::stringstream ss;
    ss << "0x" << std::hex << a;
    return ss.str();
}

std::string DbTable::DataString(RecordKey record_key) {
    char *bkt = LocateBucketLocal(record_key);
    // char *row_ptr = (char *)bucket_ptr;
    uint64_t row_size = txn_schema_.GetRecordMemSize();
    bool find = false;
    RecordHeader *record_header = nullptr;
    for (int i = 0; i < GetRecordNumPerBucket(); ++i) {
        record_header = GetRecord(bkt, i);
        if (record_header->RecordKeyMatch(record_key)) {
            find = true;
            break;
        }
    }
    if (!find) {
        return std::string();
    }

    std::stringstream col_name_ss;
    std::stringstream line_separator;
    std::stringstream data_ss;
    std::stringstream meta_ss;
    std::vector<size_t> col_display_size;

    // Output as hex with leading 0x symbol
    const size_t KeyDisplaySize = 18;
    const size_t VersionAndLockDisplaySize = 18;

    for (const auto &col : txn_schema_.GetColumnInfos()) {
        // Only display which has not that many sizes
        if (col.GetDataDisplaySize() >= 30) {
            // Do not display the data of this column
            col_display_size.push_back(std::max(col.Name().size(), VersionAndLockDisplaySize));
        } else {
            col_display_size.push_back(std::max(
                std::max(col.Name().size(), col.GetDataDisplaySize()), VersionAndLockDisplaySize));
        }
    }

    line_separator << "+" << std::string(KeyDisplaySize + 2, '-');

    for (const auto &col : txn_schema_.GetColumnInfos()) {
        line_separator << "+";
        line_separator << std::string(col_display_size[col.ColumnKey()] + 2, '-');
    }
    line_separator << "+";

    col_name_ss << "| " << MakeCenterString(KeyDisplaySize, "key") << " ";
    for (const auto &col : txn_schema_.GetColumnInfos()) {
        size_t output_size = col_display_size[col.ColumnKey()];
        col_name_ss << "| " << MakeCenterString(output_size, col.Name()) << " ";
    }
    col_name_ss << "|";

    data_ss << "| " << MakeCenterString(KeyDisplaySize, std::to_string(record_key)) << " ";
    uint64_t lock = record_header->lock;
    meta_ss << "| " << MakeCenterString(VersionAndLockDisplaySize, ToHexString(lock)) << " ";
    for (const auto &col : txn_schema_.GetColumnInfos()) {
        ColumnHeader *col_ptr =
            (ColumnHeader *)((char *)record_header + txn_schema_.ColumnOffset(col.ColumnKey()));
        size_t output_size = col_display_size[col.ColumnKey()];
        meta_ss << "| "
                << MakeCenterString(col_display_size[col.ColumnKey()],
                                    ToHexString(col_ptr->version))
                << " ";
        if (col.GetDataDisplaySize() > 30) {
            data_ss << "| " << std::string(col_display_size[col.ColumnKey()], ' ') << " ";
            continue;
        }
        switch (col.value_type) {
            case kInt32: {
                int32_t v = *(int32_t *)(col_ptr->ColumnData());
                data_ss << "| " << MakeCenterString(output_size, std::to_string(v)) << " ";
                break;
            }
            case kUInt32: {
                uint32_t v = *(uint32_t *)(col_ptr->ColumnData());
                data_ss << "| " << MakeCenterString(output_size, std::to_string(v)) << " ";
                break;
            }
            case kInt64: {
                int64_t v = *(int64_t *)(col_ptr->ColumnData());
                data_ss << "| " << MakeCenterString(output_size, std::to_string(v)) << " ";
                break;
            }
            case kUInt64: {
                uint64_t v = *(uint64_t *)(col_ptr->ColumnData());
                data_ss << "| " << MakeCenterString(output_size, std::to_string(v)) << " ";
                break;
            }
            case kFloat: {
                float v = *(float *)(col_ptr->ColumnData());
                data_ss << "| " << MakeCenterString(output_size, std::to_string(v)) << " ";
                break;
            }
            case kDouble: {
                double v = *(double *)(col_ptr->ColumnData());
                data_ss << "| " << MakeCenterString(output_size, std::to_string(v)) << " ";
                break;
            }
            case kVarChar: {
                data_ss << "| "
                        << MakeCenterString(output_size,
                                            std::string((char *)(&(col_ptr->inline_data[0]))))
                        << " ";
                break;
            }
            default: {
                data_ss << "| " << std::setw(output_size) << ' ' << " ";
                break;
            }
        }
    }
    data_ss << "|";
    meta_ss << "|";

    return line_separator.str() + "\n" + col_name_ss.str() + "\n" + line_separator.str() + "\n" +
           data_ss.str() + "\n" + line_separator.str() + "\n" + meta_ss.str() + "\n" +
           line_separator.str() + "\n";
}