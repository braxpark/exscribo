all:
	g++ -std=c++20 ${LDFLAGS} ${CPPFLAGS} -lpq -o output/main main.cpp
