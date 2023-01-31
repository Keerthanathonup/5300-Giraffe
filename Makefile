CFLAGS = -std=c++11 -lstdc++ -Wall -I../src/ -L../

all:
	$(CXX) $(CFLAGS) msql.cpp -o msql -lsqlparser