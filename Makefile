CC=gcc
CFLAGS=-g

SRCDIR=src
INCLDIR=include
LIBDIR=lib

mapreduce: $(SRCDIR)/mapreduce.c $(LIBDIR)/utils.o mapper reducer
	$(CC) $(CFLAGS) -I$(INCLDIR) $(LIBDIR)/utils.o $(SRCDIR)/mapreduce.c -o mapreduce

mapper: $(SRCDIR)/mapper.c $(LIBDIR)/utils.o
	$(CC) $(CFLAGS) -I$(INCLDIR) $(LIBDIR)/utils.o $(SRCDIR)/mapper.c -o mapper

reducer: $(SRCDIR)/reducer.c $(LIBDIR)/utils.o
	$(CC) $(CFLAGS) -I$(INCLDIR)  $(LIBDIR)/utils.o $(SRCDIR)/reducer.c -o reducer

$(LIBDIR)/utils.o: $(LIBDIR)/utils.c
	$(CC) $(CFLAGS) -I$(INCLDIR) -c $(LIBDIR)/utils.c -o $(LIBDIR)/utils.o	

.PHONY: run clean t1 t2 t3

#500KB
t1f:
	make
	./mapreduce 5 2 test/T2/empty
t1m:
	make
	./mapreduce 0 -1 test/T2/empty
t1mltr:
	make
	./mapreduce 1 2 test/T2/empty
t1r:
	make
	./mapreduce 1 0 test/T2/empty
t2:
# 	make -i clean
	make
	./mapreduce 5 2 test/T2/empty.txt
t3:
# 	make -i clean
	make
	./mapreduce 1 1 test/T3/F3.txt
t4:
# 	make -i clean
	make
	./mapreduce 32 32 test/T3/F3.txt
t5:
# 	make -i clean
	make
	./mapreduce 5 2 test/T1/F1.txt

clean:
	rm mapreduce mapper reducer
	rm -rf output

valMR:
	make
	chmod +x mapreduce
	chmod +x mapper
	chmod +x reducer
	valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes --trace-children=yes ./mapreduce 5 2 test/T1/F1.txt