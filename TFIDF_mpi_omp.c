//
//AUTHOR: Deepak Gupta (email id: dgupta22@ncsu.edu)
//

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <dirent.h>
#include <math.h>
#include "mpi.h"
#include "omp.h"
#include <sys/time.h>

#define MAX_WORDS_IN_CORPUS 15000
#define MAX_FILEPATH_LENGTH 16
#define MAX_WORD_LENGTH 16
#define MAX_DOCUMENT_NAME_LENGTH 8
#define MAX_STRING_LENGTH 64
#define NTHREADS 16

typedef char word_document_str[MAX_STRING_LENGTH];

typedef struct o
{
	char word[32];
	char document[8];
	int wordCount;
	int docSize;
	int numDocs;
	int numDocsWithWord;
	double TFIDF_value;
} obj;

typedef struct w
{
	char word[32];
	int numDocsWithWord;
	int currDoc;
} u_w;

static int myCompare(const void *a, const void *b)
{
	return strcmp(a, b);
}

int main(int argc, char *argv[])
{
	struct timeval cpu_start, cpu_end;
	/* process information */
	int numOfProcesses; //stores number of processes started
	int rank;			//stores rank of a particular process
	int tag = 50;
	MPI_Status status;

	/* initialize MPI */
	MPI_Init(&argc, &argv);

	/* get the number of procs in the comm */
	MPI_Comm_size(MPI_COMM_WORLD, &numOfProcesses);

	/* get process' rank in the comm */
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	if(rank==0)
		gettimeofday(&cpu_start, NULL);

	DIR *files;
	struct dirent *file;
	int i, j;
	int numDocs = 0, docSize, contains;
	char filename[MAX_FILEPATH_LENGTH], word[MAX_WORD_LENGTH], document[MAX_DOCUMENT_NAME_LENGTH];

	// Will hold all TFIDF objects for all documents
	obj TFIDF[MAX_WORDS_IN_CORPUS];
	obj TFIDF_atRoot[MAX_WORDS_IN_CORPUS * numOfProcesses];
	int TF_idx = 0;

	for (i = 0; i < MAX_WORDS_IN_CORPUS; i++)
		TFIDF[i].numDocsWithWord = -1;

	// Will hold all unique words in the corpus and the number of documents with that word
	u_w unique_words[MAX_WORDS_IN_CORPUS];

	//Will hold all unique words at root after gathering from all the process
	u_w unique_words_atRoot[MAX_WORDS_IN_CORPUS * numOfProcesses];
	int uw_idx = 0;
	int wordCounterAtRoot = 0;

	//initialize unique words
	for (i = 0; i < MAX_WORDS_IN_CORPUS; i++)
		unique_words[i].numDocsWithWord = -1;

	// Will hold the final strings that will be printed out
	word_document_str strings[MAX_WORDS_IN_CORPUS];

	//Count numDocs at process 0
	if (rank == 0)
	{
		//Count numDocs
		if ((files = opendir("input")) == NULL)
		{
			printf("Directory failed to open\n");
			exit(1);
		}
		while ((file = readdir(files)) != NULL)
		{
			// On linux/Unix we don't want current and parent directories
			if (!strcmp(file->d_name, "."))
				continue;
			if (!strcmp(file->d_name, ".."))
				continue;
			numDocs++;
		}
		printf("numdocs = %d numOfProcesses=%d ", numDocs, numOfProcesses);
	}

	//Broadcasts numDocs to all the processes
	MPI_Bcast(&numDocs, 1, MPI_INT, 0, MPI_COMM_WORLD);
	MPI_Barrier(MPI_COMM_WORLD);

	//workers reads their respective documents
	if (rank != 0)
	{
		// Loop through each document and gather TFIDF variables for each word
		#pragma omp parallel for num_threads(NTHREADS)
		for (int i = rank; i <= numDocs; i = i + (numOfProcesses - 1))
		{
			sprintf(document, "doc%d", i);
			sprintf(filename, "input/%s", document);
			FILE *fp = fopen(filename, "r");
			if (fp == NULL)
			{
				printf("Error Opening File: %s\n", filename);
				//exit(0);
				continue;
			}

			// Get the document size
			docSize = 0;
			while ((fscanf(fp, "%s", word)) != EOF)
				docSize++;

			// For each word in the document
			fseek(fp, 0, SEEK_SET);
			while ((fscanf(fp, "%s", word)) != EOF)
			{
				contains = 0;

				// If TFIDF array already contains the word@document, just increment wordCount and break
				for (j = 0; j < TF_idx; j++)
				{
					if (!strcmp(TFIDF[j].word, word) && !strcmp(TFIDF[j].document, document))
					{
						contains = 1;
						TFIDF[j].wordCount++;
						break;
					}
				}

				//If TFIDF array does not contain it, make a new one with wordCount=1
				if (!contains)
				{
					strcpy(TFIDF[TF_idx].word, word);
					strcpy(TFIDF[TF_idx].document, document);
					TFIDF[TF_idx].wordCount = 1;
					TFIDF[TF_idx].docSize = docSize;
					TFIDF[TF_idx].numDocs = numDocs;
					TF_idx++;
				}

				contains = 0;
				// If unique_words array already contains the word, just increment numDocsWithWord
				for (j = 0; j < uw_idx; j++)
				{
					if (!strcmp(unique_words[j].word, word))
					{
						contains = 1;
						if (unique_words[j].currDoc != i)
						{
							unique_words[j].numDocsWithWord++;
							unique_words[j].currDoc = i;
						}
						break;
					}
				}

				// If unique_words array does not contain it, make a new one with numDocsWithWord=1
				if (!contains)
				{
					strcpy(unique_words[uw_idx].word, word);
					unique_words[uw_idx].numDocsWithWord = 1;
					unique_words[uw_idx].currDoc = i;
					uw_idx++;
				}
			}
			fclose(fp);
		} //end of for
	}	 //end rank != 0

	MPI_Barrier(MPI_COMM_WORLD);

	//master gathers unique array from all workers
	MPI_Gather(&unique_words, sizeof(u_w) * MAX_WORDS_IN_CORPUS, MPI_BYTE,
			   &unique_words_atRoot, sizeof(u_w) * MAX_WORDS_IN_CORPUS, MPI_BYTE,
			   0, MPI_COMM_WORLD);

	//master collaborates unique words array
	if (rank == 0)
	{
		for (j = 0; j < MAX_WORDS_IN_CORPUS * numOfProcesses; j++)
		{
			if (unique_words_atRoot[j].numDocsWithWord != -1)
			{
				for (i = j + 1; i < MAX_WORDS_IN_CORPUS * numOfProcesses; i++)
				{
					if ((unique_words_atRoot[i].numDocsWithWord != -1) && !strcmp(unique_words_atRoot[i].word, unique_words_atRoot[j].word))
					{
						unique_words_atRoot[j].numDocsWithWord += unique_words_atRoot[i].numDocsWithWord;
						unique_words_atRoot[i].numDocsWithWord = -1;
					}
				}
			}
		}

		for (j = 0; (j < MAX_WORDS_IN_CORPUS * numOfProcesses); j++)
		{
			if (unique_words_atRoot[j].numDocsWithWord != -1)
			{
				strcpy(unique_words[wordCounterAtRoot].word, unique_words_atRoot[j].word);
				unique_words[wordCounterAtRoot].numDocsWithWord = unique_words_atRoot[j].numDocsWithWord;
				wordCounterAtRoot++;
			}
		}
	}

	//master broadcasts the collaborated array to workers
	MPI_Bcast(&unique_words, sizeof(u_w) * MAX_WORDS_IN_CORPUS, MPI_BYTE, 0, MPI_COMM_WORLD);

	//workers use the received unique_words array to calculate TFIDF
	if (rank != 0)
	{
		#pragma omp parallel for num_threads(NTHREADS)
		// Use unique_words array to populate TFIDF objects with: numDocsWithWord
		for (int i = 0; i < TF_idx; i++)
		{
			for (int j = 0; j < MAX_WORDS_IN_CORPUS; j++)
			{
				if (unique_words[j].numDocsWithWord != -1 && !strcmp(TFIDF[i].word, unique_words[j].word))
				{
					TFIDF[i].numDocsWithWord = unique_words[j].numDocsWithWord;
					break;
				}
			}
		}

		#pragma omp parallel for num_threads(NTHREADS)
		// Calculates TFIDF value and puts: "document@word\tTFIDF" into strings array
		for (int j = 0; j < TF_idx; j++)
		{
			double TF = 1.0 * TFIDF[j].wordCount / TFIDF[j].docSize;
			double IDF = log(1.0 * TFIDF[j].numDocs / TFIDF[j].numDocsWithWord);
			TFIDF[j].TFIDF_value = TF * IDF;
		}
	}

	//master gathers TFIDF array from all workers
	MPI_Gather(&TFIDF, sizeof(obj) * MAX_WORDS_IN_CORPUS, MPI_BYTE,
			   &TFIDF_atRoot, sizeof(obj) * MAX_WORDS_IN_CORPUS, MPI_BYTE,
			   0, MPI_COMM_WORLD);

	if(rank==0)
	{
		gettimeofday(&cpu_end, NULL);
		double elapsed_cpu= ((cpu_end.tv_sec + cpu_end.tv_usec * 1e-6)-(
					cpu_start.tv_sec + cpu_start.tv_usec * 1e-6));
		printf("total time taken is %f seconds\n", elapsed_cpu);
	}

	//master collaborates TFIDF array
	//and then writes to the output file
	if (rank == 0)
	{
		int stringsCounter = 0;

		#pragma omp parallel for num_threads(NTHREADS)
		for (i = 0; i < MAX_WORDS_IN_CORPUS * numOfProcesses; i++)
		{
			if(TFIDF_atRoot[i].numDocsWithWord != -1)
			{
				sprintf(strings[stringsCounter], "%s@%s\t%.16f\n", TFIDF_atRoot[i].document, TFIDF_atRoot[i].word, TFIDF_atRoot[i].TFIDF_value);
				stringsCounter++;
			}
		}

		TF_idx = stringsCounter;

		//sorts the calculated TFIDF values
		qsort(strings, TF_idx, sizeof(char) * MAX_STRING_LENGTH, myCompare);

		//writes the TFIDF values to output file
		FILE *fp = fopen("output.txt", "w");
		if (fp == NULL)
		{
			printf("Error Opening File: output.txt\n");
			exit(0);
		}

		for (i = 0; i < TF_idx; i++)
			fprintf(fp, "%s", strings[i]);

		fclose(fp);
	}

	MPI_Finalize();
	return 0;
}
