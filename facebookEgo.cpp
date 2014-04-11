#include<climits>
#include<fstream>
#include<cmath>
#include<algorithm>
#include<iostream>
#include<omp.h>
#define d 0.85
#define epsilon 0.0000001

using namespace std;

int main() {
    ifstream input("facebook_combined.txt");
    ofstream out("task_1.txt");
    int a, b, i, j;
    int count1 =0;
    int tid, nthreads;
    int max = INT_MIN;
    int nValue = 0;
    int chunk = 500;
    double sumOfErrorSquared = 0.0;
    bool flag = false;
    while (input >> a >> b) {
        max = a > max ? a : max;
        max = b > max ? b : max;
    }
    printf("%d", max);
    input.clear();
    input.seekg(0, input.beg);
    max++;
    const int size = max;
    double **adjacencyMatrix;
    adjacencyMatrix = new double*[size];
    for(i=0;i<size;i++)
    {
    	adjacencyMatrix[i] = new double[size];
    }
    double oldPageRank[size],newPageRank[size];
    int count[size];
    //------------------------Initialize the adjacency matrix to zero---------------------------
    for (i = 0; i < size; i++) {
        for (j = 0; j < size; j++) {
            adjacencyMatrix[i][j] = 0;
        }
        oldPageRank[i] = 1.0;
        newPageRank[i] = 0.0;
        count[i] = 0;
    }
    //---------------------Read the input file and set adjacency matrix--------------------------
    while (input >> a >> b) {
        adjacencyMatrix[a][b] = 1;
        adjacencyMatrix[b][a] = 1;
        nValue++;
    }
    nValue *= 2;
    input.close();
    
    //------------------------------Normalize the adjacency matrix-------------------------------
    for(j=0;j<size;j++)
    {
    	for(i=0;i<size;i++)
    	{
    		count[j] = adjacencyMatrix[i][j]==1 ? count[j]+1:count[j];
    	}
    }
    
    for(j=0;j<size;j++)
    {
    	for(i=0;i<size;i++)
    	{
    		if(count[j]!=0)
    			adjacencyMatrix[i][j] = adjacencyMatrix[i][j]/count[j];
    	}
    }
    #pragma omp parallel shared(max,adjacencyMatrix,nthreads,chunk,oldPageRank,newPageRank,count1) private(tid,i,j)
    {
        tid = omp_get_thread_num();
        if (tid == 0) {
            nthreads = omp_get_num_threads();
        }
				#pragma omp for schedule (static,chunk)	
        	for (j = 0; j < size; j++) {
            	oldPageRank[j] = 1.0/(max);
        	}
		
        while (!flag) {
            count1++;
        		sumOfErrorSquared = 0.0;
        		#pragma omp for schedule (static,chunk)
            for (i = 0; i < size; i++) {
            		newPageRank[i] = 0;
                for (j = 0; j < size; j++) {
                    newPageRank[i] = newPageRank[i] + (adjacencyMatrix[i][j] * oldPageRank[j]);
                }
                newPageRank[i] = ((1-d)/(max))+ (d*newPageRank[i]);
            }
           	#pragma omp for schedule (static,chunk)
            for (i = 0; i < max; i++) {
                sumOfErrorSquared += pow(newPageRank[i] - oldPageRank[i], 2);
            }
            #pragma omp for schedule (static,chunk)
            for(i=0;i<max;i++){
            	oldPageRank[i] = newPageRank[i];
            }
            flag = sumOfErrorSquared > (max) * pow(epsilon, 2) ? false : true;
            printf("%d\n",count1);
        }
     }
    
    //sort(newPageRank,newPageRank+size);
    int min = count[0];
    int minj = 0;
    for (i = 0; i < size; i++) {
        out << i << "\t" << newPageRank[i]<<endl;
        min = count[i]<min ? count[i]:min;
        minj = count[i]==min ? i:minj;
    }
    
    return 0;
}
