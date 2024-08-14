all:
	g++ -std=c++20 ${LDFLAGS} ${CPPFLAGS} -lpq -o main main.cpp
