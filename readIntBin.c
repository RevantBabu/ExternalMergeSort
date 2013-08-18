#include <stdio.h>
#include <stdlib.h>


#define BLOCK_SIZE 4096
#define BUFFERS 100
int main(){
	int num1;
	int count;
	int attributes[50][2];
	double num2;
	char data2[50];
	FILE *ptr_myfile;
	FILE *ptr_myfile1;
	//Location of bin file

   	char path[64];
   	printf("Enter no. file path to read : ");
   	scanf("%s",path);
   	int rn;
    printf("Enter no. rows to read : ");
    scanf("%d",&rn);

	ptr_myfile=fopen(path, "rb");
	if (!ptr_myfile)
	{
		printf("Unable to open file!");
		return 1;
	}

	int memloc=0;
	unsigned int noofattr;
	int type,length;
	fread(&noofattr,sizeof(unsigned int),1,ptr_myfile);
	//printf("No of attributes: %d",noofattr);
	//printf("\n\n----------Metadata---------------\n\n");
	for(count=0;count<noofattr;count++){
		fread(&type,sizeof(int),1,ptr_myfile);
		fread(&length,sizeof(int),1,ptr_myfile);
		attributes[count][0]=type;
		attributes[count][1]=length;
		//printf("Attribute %d\ntype: %d length: %d\n\n",count+1,type,length);
	}



	// sprintf(path, "/home/revanth/Desktop/temp/SortedRuns/run_%d_out%d.bin", ln, fn);
	// ptr_myfile1=fopen(path,"rb");
	// if (!ptr_myfile1)
	// {
	// 	printf("Unable to open file!");
	// 	return 1;
	// }

	int returnval=1;
    int iter = 0;
	while(returnval && iter< rn)
	{		
		for(count=0;count<noofattr;count++)
		{
			switch (attributes[count][0]){
			case 1: returnval=fread(&num1,attributes[count][1],1,ptr_myfile);
					if (returnval) printf("%d ",num1);
					break;
			case 2: returnval=fread(&num2,attributes[count][1],1,ptr_myfile);
					if (returnval) printf("%f ",num2);
					break;
			case 3: returnval=fread(data2,attributes[count][1],1,ptr_myfile);					
					if (returnval) printf("%s ",data2);
					break;
			}			
		}
		if (returnval) printf("\n");
		iter++;
	}
	printf("num records %d\n",iter);
}
