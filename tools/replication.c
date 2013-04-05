#include <stdio.h>
#include <strings.h>

int buckets[1440];
int counts[1440];

int main()
{
  FILE *f=fopen("data/test.csv","r");
  FILE *out=fopen("data/replication.csv","w");
  
  bzero(buckets,sizeof(1440)*sizeof(int));
  bzero(counts,sizeof(1440)*sizeof(int));

  fprintf(out,"time;count\n");

  char line[1024];
  char lastbid[1024]="";
  long long lastbidfirsttime=0;
  int lastdelay=0;
  int lastcount=0;

  line[0]=0; fgets(line,1024,f);
  while (line[0]) {
    line[1023]=0;
    char bid[1024];
    long long timestamp;
    int count;

    if (sscanf(line,"%[^;];%*[^;];%lld;%d",
	       bid,&timestamp,&count)==3) {
      if (strcmp(bid,lastbid)) {
	strcpy(lastbid,bid);
	lastbidfirsttime=timestamp;
	lastdelay=0;
	lastcount=0;
      }
      int delay=(timestamp-lastbidfirsttime)/60000;
      if (delay>=0&&delay<1440) {
	for(;lastdelay<delay;lastdelay++) {
	  buckets[lastdelay]+=lastcount;
	  counts[lastdelay]++;
	}
	lastcount=count;
      }

      fprintf(out,"%lld;%d\n",
	      timestamp-lastbidfirsttime,count);
    }

    line[0]=0; fgets(line,1024,f);
  }
  fclose(out);
  out=fopen("data/replicationcurve.csv","w");
  int i;
  fprintf(out,"minutes;count\n");
  for(i=0;i<1440;i++)
    if (counts[i])
      fprintf(out,"%d;%f\n",i,buckets[i]*1.0/counts[i]);
  fclose(out);
  
  return 0;
}
