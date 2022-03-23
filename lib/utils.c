#include "utils.h"

char *getChunkData(int mapperID) {
  
  
  //TODO open message queue
   


  //TODO receive chunk from the master
    


  //TODO check for END message and send ACK to master and return NULL. 
  //Otherwise return pointer to the chunk data. 
  //
}


//Return the next word as an output parameter.
//Return 1: it reads a word. Return 0: it reaches the end of the
//stream. 
int getNextWord(int fd, char* buffer){
   char word[100];
   memset(word, '\0', 100);
   int i = 0;
   while(read(fd, &word[i], 1) == 1 ){
    if(word[i] == ' '|| word[i] == '\n' || word[i] == '\t'){
        strcpy(buffer, word);
        return 1;
    }
    if(word[i] == 0x0){
      break;
    }

    i++;
   }
   strcpy(buffer, word);
   return 0;
}


void sendChunkData(char *inputFile, int nMappers) {

  // Declaring Variables
  int mid, fd;
  int mapperID = 0;
  key_t key = 100;
  struct msgBuffer msg;
  char chunkBuffer[MSGSIZE];
  char wordBuffer[MSGSIZE];

  // Opening the Message Queue
  mid = msgget(key, 0666 | IPC_CREAT);

  // exit if mssget fails
  if(mid == -1) {
    printf("ERROR: Unable to open message queue\n");
		exit(0);
	}
  
  // Open input file to parse
  fd = open(inputFile, O_RDONLY);

  // exit if unable to open input file
	if (fd < 0){
		printf("ERROR: Cannot open the file %s\n", inputFile);
		exit(0);
	}

  // Send data to all mappers using the msg queue
  while(getNextWord(fd, wordBuffer)!=0){
    if(strlen(chunkBuffer) + strlen(wordBuffer) > 1024){
        //sending the chunk to mapperID
        msg.msgType = mapperID +1;
        mapperID = (mapperID+1)%nMappers;
        strcpy(msg.msgText,chunkBuffer);
        strcpy(chunkBuffer,wordBuffer);
        // Sending msg to queue with error handeling
        if((msgsnd(mid, (void *) &msg, sizeof(msg.msgText), 0)) == -1){
          printf("ERROR: Unable to send msg chunk to mapper id: %i \n", mapperID);
          exit(0);
        }
        // Reset our buffers for next mapper
        memset(msg.msgText, '\0', MSGSIZE);
        memset(wordBuffer, '\0', MSGSIZE);
        memset(chunkBuffer, '\0', MSGSIZE);
        
    }else{
        //concatennate the word to the chunk buffer
        strcat(chunkBuffer,wordBuffer);
        // Reset our word buffer
        memset(wordBuffer, '\0', MSGSIZE);
    }
  }

  // Sending remaining words in wordBuffer to msg queue if end of file reached
  msg.msgType = mapperID +1;
  strcat(chunkBuffer,wordBuffer);
  strcpy(msg.msgText,chunkBuffer);
  // Sending msg to queue with error handeling
  if((msgsnd(mid, (void *) &msg, sizeof(msg.msgText), 0)) == -1){
    printf("ERROR: Unable to send msg chunk to mapper id: %i \n", mapperID);
    exit(0);
  }

  // Close file
  close(fd);

  // Sending END message to mappers
  memset(msg.msgText, '\0', MSGSIZE);
  for(int i = 1; i <= nMappers; i++){
    msg.msgType = i;
    sprintf(msg.msgText, "");
    if((msgsnd(mid, (void *) &msg, sizeof(msg.msgText), 0)) == -1){
      printf("ERROR: Unable to send END msg to mapper id: %i \n", i);
      exit(0);
    }
  }

  // Waiting to recieve ACK from all mappers for END notification
  for(int i = 1; i<= nMappers; i++){
    if ((msgrcv (mid, (void *)&msg, sizeof (msg), i, 0)) == -1) {
      printf("ERROR: Unable to recieve ACK from mapper id: %i \n", i);
      exit(0);
    }
  }

  // Closing Message Queue
  close(mid);
  // msgctl(mid, IPC_RMID, 0);

}

// hash function to divide the list of word.txt files across reducers
//http://www.cse.yorku.ca/~oz/hash.html
int hashFunction(char* key, int reducers){
	unsigned long hash = 0;
    int c;

    while ((c = *key++)!='\0')
        hash = c + (hash << 6) + (hash << 16) - hash;

    return (hash % reducers);
}

int getInterData(char *key, int reducerID) {
    //TODO open message queue
      



    //TODO receive data from the master
     



    //TODO check for END message and send ACK to master and then return 0
    //Otherwise return 1


}

void shuffle(int nMappers, int nReducers) {

    //TODO open message queue
     


    //TODO traverse the directory of each Mapper and send the word filepath to the reducer
    //You should check dirent is . or .. or DS_Store,etc. If it is a regular
    //file, select the reducer using a hash function and send word filepath to
    //reducer 




    //TODO inputFile read complete, send END message to reducers

    


    
    //TODO  wait for ACK from the reducers for END notification
}

// check if the character is valid for a word
int validChar(char c){
	return (tolower(c) >= 'a' && tolower(c) <='z') ||
					(c >= '0' && c <= '9');
}

char *getWord(char *chunk, int *i){
	char *buffer = (char *)malloc(sizeof(char) * chunkSize);
	memset(buffer, '\0', chunkSize);
	int j = 0;
	while((*i) < strlen(chunk)) {
		// read a single word at a time from chunk
		// printf("%d\n", i);
		if (chunk[(*i)] == '\n' || chunk[(*i)] == ' ' || !validChar(chunk[(*i)]) || chunk[(*i)] == 0x0) {
			buffer[j] = '\0';
			if(strlen(buffer) > 0){
				(*i)++;
				return buffer;
			}
			j = 0;
			(*i)++;
			continue;
		}
		buffer[j] = chunk[(*i)];
		j++;
		(*i)++;
	}
	if(strlen(buffer) > 0)
		return buffer;
	return NULL;
}

void createOutputDir(){
	mkdir("output", ACCESSPERMS);
	mkdir("output/MapOut", ACCESSPERMS);
	mkdir("output/ReduceOut", ACCESSPERMS);
}

char *createMapDir(int mapperID){
	char *dirName = (char *) malloc(sizeof(char) * 100);
	memset(dirName, '\0', 100);
	sprintf(dirName, "output/MapOut/Map_%d", mapperID);
	mkdir(dirName, ACCESSPERMS);
	return dirName;
}

void removeOutputDir(){
	pid_t pid = fork();
	if(pid == 0){
		char *argv[] = {"rm", "-rf", "output", NULL};
		if (execvp(*argv, argv) < 0) {
			printf("ERROR: exec failed\n");
			exit(1);
		}
		exit(0);
	} else{
		wait(NULL);
	}
}

void bookeepingCode(){
	removeOutputDir();
	sleep(1);
	createOutputDir();
}
