all:
	mpicc -o TFIDF TFIDF.c -lm -O3

clean:
	rm -f TFIDF
