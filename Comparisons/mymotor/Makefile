release: clean
	cmake -B build -DCMAKE_BUILD_TYPE=Release
	cmake --build build -j 

debug: clean
	cmake -B build -DCMAKE_BUILD_TYPE=Debug
	cmake --build build -j 

relwithdeb: clean
	cmake -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
	cmake --build build -j 

clean:
	rm -rf build