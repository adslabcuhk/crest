# The timeout threshold for a single command 
execCmdTimeout = 300

# The separator time between two "run_once" call
roundSep = 10

# Configure your context to run the benchmark here:
# 1. The absolute path to the project where you download it:
projectPath = "/path/to/your/project"

username = "yourusrname"
passwd = "yourpasswd"

# 2. Node information:
# configure the ip of your CNs and MNs in the following format: 
#  [("ip1", id1), ("ip2", id2), ("ip3", id3), ...]
cns = [("10.118.0.36", 0), ("10.118.0.40", 1), ("10.118.0.42", 2)]
mns = [("10.118.0.45", 0), ("10.118.0.49", 1)]
