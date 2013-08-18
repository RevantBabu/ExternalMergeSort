#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#define BLOCK_SIZE 4096
#define COLUMNS 2
#define LOGCONSTANT 64


/* global variable used during different phases*/
int NUMBUFFERS;

int** metaData;
int recordSize;
int BUFFER_SIZE;

unsigned int numAttributes;
int numSortAttributes;
int* sortAttributes;
int tempfile;
int level;

char outFilename[256];
char * runs_path;

/* returns the min/max record from available blocks during merge*/
int getFirst(char** c, int* status)
{
	int iter;
	char min[recordSize];
	int i = 0;
	int res;
	while (i<NUMBUFFERS && status[i] == -1)
	{
		i++;
	}
	if (i==NUMBUFFERS)
		return -1;
	else
	{
		res = i;
		memcpy(min, c[i]+(status[i]*recordSize), recordSize);
	}

	for (iter=i; iter<NUMBUFFERS; iter++)
	{
		if (status[iter] != -1 && compare((void*)min, (void*)(c[iter]+(status[iter]*recordSize))) > 0)
		{
			memcpy(min, c[iter]+(status[iter]*recordSize), recordSize);
			res = iter;
		}
	}
	return res;
}

/* reads the meta data ,attribute information from input file*/
void getMetaData(FILE* filePtr)
{
	int iter;
	
	fread(&numAttributes, sizeof(unsigned int), 1, filePtr);
	//printf("No of attributes: %d \n", numAttributes);
	//printf("----------Metadata---------------\n");

	metaData = malloc(numAttributes*sizeof(int *));
	for(iter = 0; iter < numAttributes; iter++)
		metaData[iter] = malloc(COLUMNS*sizeof(int));

	recordSize = 0;

	for(iter=0; iter<numAttributes; iter++)
	{
		fread(&metaData[iter][0], sizeof(int), 1, filePtr);
		fread(&metaData[iter][1], sizeof(int), 1, filePtr);
		recordSize += metaData[iter][1];
		//printf("Attribute %d --- type: %d length: %d \n", iter+1, metaData[iter][0], metaData[iter][1]);
	}
	//printf("recordSize : %d \n", recordSize);
}

/*used to get position of attribute */
int getpos(int n)
{
	int pos = 0,iter;
	for (iter=0; iter<n; iter++)
	{
		pos+=metaData[iter][1];
	}
	return pos;
}

/*compares two records based on sort info presented*/
int compare (const void * a, const void * b)
{
  char const *aa = (char const *)a;
  char const *bb = (char const *)b;

  int iter1, iter2, order, res = 0;
  
  for (iter1 = 0;iter1 < numSortAttributes; iter1++)
  {
  	
  	order = sortAttributes[iter1]<0 ? -1:1 ;
  	
  	int pos = getpos(abs(sortAttributes[iter1]) - 1);
  	int size = metaData[iter1][1];
  	
  	char s1[size];
  	char s2[size];
	char s3[size+1];
	char s4[size+1];

  	for (iter2=pos; iter2<pos+size; iter2++)
  		{
  			s1[iter2-pos] = aa[iter2];
  			s2[iter2-pos] = bb[iter2];
  		}

  	switch (metaData[abs(sortAttributes[iter1])-1][0])
  	{
  		case 1:
  			res = *(int*)s1 - *(int*)s2;
			break;
		case 2:	
		 	if (*(float*)s1 - *(float*)s2 == 0)
		 		res = 0;
		 	else if (*(float*)s1 - *(float*)s2 > 0)
		 		res = 1;
		 	else 
		 		res = -1;
			break;	
		case 3:
			strncpy(s3,s1,size);
			s3[size] = '\0';
			strncpy(s4,s2,size);
			s4[size] = '\0';
			res = strcmp(s3,s4);
			break;
  	}

  	if (res != 0)
  	  break;	
  }

  return res*order;	
}

/*merging sorted runs*/
void mergeSortedRuns(int level, int numFiles, char* outfile)
{
	if (numFiles == 1)
		return ;
	int run = 0;
	int iter;
	int status[NUMBUFFERS];
	int maxStatus[NUMBUFFERS];
	FILE* fpArray[NUMBUFFERS];
	int fileNum = 0;
	int numRecords = BUFFER_SIZE/recordSize;
	int returnval;

	while (numFiles > 0)
	{
		char** logBlocks;//[NUMBUFFERS][BUFFER_SIZE];
		//char outBuffer[BUFFER_SIZE];
		char* outBuffer;
		outBuffer = malloc(BUFFER_SIZE*sizeof(char));
		int outStatus = 0;

		logBlocks = malloc(NUMBUFFERS*sizeof(char*));
		for(iter = 0; iter < NUMBUFFERS; iter++)
			logBlocks[iter] = malloc(BUFFER_SIZE*sizeof(char));

		for (iter=0; iter<NUMBUFFERS; iter++)
		{
			status[iter] = 0;
			maxStatus[iter] = 0;

		   	char path[256];
		   	sprintf(path, "%s/run_%d_out%d.bin",runs_path, level-1, fileNum);

			fpArray[iter] = fopen(path,"rb");
			if (!fpArray[iter])
			{
				//printf("Unable to open file %d!, level %d \n", fileNum, level-1);
				status[iter] = -1;
				maxStatus[iter] = 0;
			}
			else
			{
				returnval = fread(logBlocks[iter], sizeof(char)*1, BUFFER_SIZE, fpArray[iter]);
				if (returnval != 0)
					maxStatus[iter] = returnval/recordSize;
			}
			
			fileNum++;
		}

		char path[256]; 
		sprintf(path, "%s/run_%d_out%d.bin",runs_path, level, run);
		FILE* outPtr;
		if (numFiles < NUMBUFFERS && run==0)
		{
				outPtr = fopen(outfile, "wb");
				fwrite((void*)(&numAttributes), sizeof(unsigned int), 1, outPtr);
				int it1;
				for(it1=0; it1<numAttributes; it1++)
				{
					fwrite((void*)(&metaData[it1][0]), sizeof(int), 1, outPtr);
					fwrite((void*)(&metaData[it1][1]), sizeof(int), 1, outPtr);
				}
		}
		else
			outPtr = fopen(path, "wb");

		while (1)
		{	
			int first = getFirst(logBlocks, status);
			if (first == -1) 
			{
				if (outStatus!=0)
				{
					fwrite(outBuffer, outStatus*recordSize, 1, outPtr);
				 	//fclose(outPtr);
				}
				break;
			}
			memcpy(outBuffer+(outStatus*recordSize), logBlocks[first]+(status[first]*recordSize), recordSize);
			status[first]++;
			if (status[first] == maxStatus[first]) //maxStatus[first])
			{
				returnval = fread(logBlocks[first], 1, BUFFER_SIZE, fpArray[first]);
				if (returnval == BUFFER_SIZE)
				{
					status[first] = 0;
					maxStatus[first] = numRecords;
				}
				else if (returnval != 0)
				{
					status[first] = 0;
					maxStatus[first] = returnval/recordSize;
				}
				else 
				{
					status[first] = -1;
					maxStatus[first] = 0;
					fclose(fpArray[first]);
				}
			}

			outStatus++;

			if (outStatus == numRecords)
			{
				fwrite(outBuffer, sizeof(char)*BUFFER_SIZE, 1, outPtr);
				outStatus = 0;
			}	
		}
		fclose(outPtr);
		numFiles -= NUMBUFFERS;
		run++;
	}

	level++;
	//printf("%d, %d\n",level, run);
	mergeSortedRuns(level, run, outfile);
}

/*first step creates the sorted runs from input file*/
void genSortedRuns(char* infile, char* outfile, int numAttributes, int sortAttributes[], int totalBufferSize)
{
	struct stat st = {0};
	runs_path = (char*)malloc(sizeof(char)*64);
	if (stat("SortedRuns", &st) == -1) {
	    int failure = mkdir("SortedRuns", 0777);
	    if(failure)runs_path="";
	    else runs_path="SortedRuns";
	}

	FILE *inPtr;
	inPtr = fopen(infile, "rb");

	getMetaData(inPtr);

	BUFFER_SIZE = recordSize*LOGCONSTANT;
	NUMBUFFERS = totalBufferSize/BUFFER_SIZE;
	int returnval = 1;
    char path[256];

    int logSize = BUFFER_SIZE*NUMBUFFERS;
    int numRecords = logSize/recordSize;

	tempfile = 0;
	FILE *runPtr;
    while(1)
    {    	
		char* logBlock;
		logBlock = (char*)malloc(sizeof(char)*logSize);
	    returnval = fread(logBlock, sizeof(char)*1, logSize, inPtr);

	    if (returnval == logSize)
	    {
	    	sprintf(path, "%s/run_0_out%d.bin", runs_path, tempfile);
        	runPtr = fopen(path, "wb");
	    	qsort (logBlock, numRecords, sizeof(char)*recordSize, compare);
        	fwrite(logBlock, sizeof(char)*logSize, 1, runPtr);
			fclose (runPtr);
	    }
	    else if (returnval != 0)
	    {
	    	sprintf(path, "%s/run_0_out%d.bin", runs_path, tempfile);
        	runPtr = fopen(path, "wb");
	    	qsort (logBlock, returnval/recordSize, sizeof(char)*recordSize, compare);
        	fwrite(logBlock, sizeof(char)*returnval, 1, runPtr);
			fclose (runPtr);
	    }
	    else if (returnval == 0)
	    	break;

        tempfile++;
 	}

}

int main()
{

	char inFilename[256];
	char outFilename[256];
	int iter;
	int totalBufferSize;
	level = 1;

	printf("Enter the name of file to be sorted : ");
	scanf("%s", inFilename);

	printf("Enter the name of output file : ");
	scanf("%s", outFilename);

	printf("Enter the number of attributes to be soted on : ");
	scanf("%d", &numSortAttributes);

	sortAttributes = (int*)malloc(sizeof(int)*numSortAttributes);

	printf("Enter the attributes to be soted on : \n");
	for (iter = 0; iter < numSortAttributes; iter++)
	{
		scanf("%d", &sortAttributes[iter]);
	}

	printf("Enter the bufferSize in Bytes : ");
	scanf("%d", &totalBufferSize);	

	genSortedRuns(inFilename, outFilename, numSortAttributes, sortAttributes, totalBufferSize);
	mergeSortedRuns(level, tempfile, outFilename);
}
