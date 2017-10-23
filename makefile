CC=g++
LIBS=-laerospike -lssl -lm -lrt
all:
	$(CC) *.cpp *.hpp $(LIBS)

.PHONY:clean
clean:
	rm -rf a.out
