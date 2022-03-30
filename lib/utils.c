#include "utils.h"

#define PERMS 0666
key_t keyID = 4061;
key_t keyID2 = 4062;

char *getChunkData(int mapperID) {
  fflush(stdout);
  // printf("get for MapID %d\n", mapperID);
//TODO open message queue
  struct msgBuffer message;
  int mid, mid2;
 
  if( (mid = msgget(keyID, PERMS | IPC_CREAT)) == -1){
    fprintf(stderr, "Problem with Mapper %d: %s", mapperID, strerror(errno));
    exit(-1);
  }
 
  if( (mid2 = msgget(keyID2, PERMS | IPC_CREAT)) == -1){
    fprintf(stderr, "Problem with Mapper %d: %s", mapperID, strerror(errno));
    exit(-1);
  }

  //TODO receive chunk from the master
  if( (msgrcv(mid, &message, sizeof(message.msgText), mapperID, 0)) == -1){
    fprintf(stderr, "Problem with Mapper %d: %s", mapperID, strerror(errno));
    exit(-1);
  }


  //TODO check for END message and send ACK to master and return NULL. 
  //Otherwise return pointer to the chunk data. 

  // Check for END message sent
  if(message.msgText[0] == 4){
    // printf(" End of File reached for MapID: %d\n", mapperID);
    // Send ACK
    memset(message.msgText, '\0', MSGSIZE);
    message.msgType = mapperID;

    if(msgsnd(mid2, &message, MSGSIZE, 0) == -1){
      fprintf(stderr, "Problem with Mapper %d: %s", mapperID, strerror(errno));
      exit(-1);
    }

    return NULL;
  }
  else{
    char *text = (char *) malloc((chunkSize+1) * sizeof(char));
    memset(text, '\0', chunkSize +1);
    strcpy(text, message.msgText);
    return text;
  }
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
  int mid, mid2, fd;
  int mapperID = 0;
  
  struct msgBuffer msg;
  char chunkBuffer[chunkSize];
  char wordBuffer[chunkSize];
  memset(chunkBuffer, '\0', chunkSize);
  memset(wordBuffer, '\0', chunkSize);

  // Opening the Message Queue and exit if mssget fails
  if( (mid = msgget(keyID, PERMS | IPC_CREAT)) == -1){
    printf("ERROR: Unable to open message queue\n");
    exit(0);
  }
 
  if( (mid2 = msgget(keyID2, PERMS | IPC_CREAT)) == -1){
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

int count = 0;
int curMapper = 1;
  // Send data to all mappers using the msg queue
  while(getNextWord(fd, wordBuffer)!=0){
    if(curMapper > nMappers){
      curMapper = 1;
    }
    printf("count: %d\n", count);
    if((count + strlen(wordBuffer))  >= 1023){  //strlen(chunkBuffer) + strlen(wordBuffer) > 1024
        //sending the chunk to mapperID
        memset(msg.msgText, '\0', MSGSIZE);
        snprintf(msg.msgText, chunkSize, "%s", chunkBuffer);
        msg.msgType = curMapper;
        // strcpy(msg.msgText,chunkBuffer);
        
        // Sending msg to queue with error handeling
        if((msgsnd(mid, &msg, MSGSIZE, 0)) == -1){
          printf("ERROR: Unable to send msg chunk to mapper id: %i \n", mapperID);
          exit(0);
        }
        // Reset our buffers for next mapper
        
        // memset(chunkBuffer, '\0', MSGSIZE);
        memset(chunkBuffer, '\0', chunkSize);
        strcpy(chunkBuffer,wordBuffer);
        count = 0;
        curMapper ++;
        memset(wordBuffer, '\0', chunkSize);
        


        
    }else{
        //concatennate the word to the chunk buffer
        strcat(chunkBuffer,wordBuffer);
        // snprintf(chunkBuffer, chunkSize, "%s", wordBuffer);
        // Reset our word buffer
        memset(wordBuffer, '\0', chunkSize);
        count += strlen(chunkBuffer);
    }
  }

  // Sending remaining words in wordBuffer to msg queue if end of file reached
  // printf("ENd of file for send \n");
  msg.msgType = mapperID +1;
  strcat(chunkBuffer,wordBuffer);
  strcpy(msg.msgText,chunkBuffer);
  // Sending msg to queue with error handeling
  if((msgsnd(mid, &msg, MSGSIZE, 0)) == -1){
    printf("ERROR: Unable to send msg chunk to mapper id: %i \n", mapperID);
    exit(0);
  }

  // Close file
  close(fd);

  // Sending END message to mappers
  
  for(int i = 1; i <= nMappers; i++){
    msg.msgType = i;
    memset(msg.msgText, '\0', MSGSIZE);
    sprintf(msg.msgText, "%c", 4);
    if((msgsnd(mid, &msg, 1, 0)) == -1){
      printf("ERROR: Unable to send END msg to mapper id: %i \n", i);
      exit(0);
    }
  }

  // Waiting to recieve ACK from all mappers for END notification
  for(int i = 1; i<= nMappers; i++){
    if ((msgrcv (mid2, &msg, MSGSIZE, 0, 0)) == -1) {
      printf("ERROR: Unable to recieve ACK from mapper id: %i \n", i);
      exit(0);
    }
  }

  // Closing Message Queue
  // close(mid);
  msgctl(mid, IPC_RMID, NULL);

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
  // Declaring Variables
  int mid, mid2, fd;
  int mapperID = 0;

  struct msgBuffer msg;

  // Opening the Message Queue and exit if mssget fails
  if( (mid = msgget(keyID, PERMS | IPC_CREAT)) == -1){
    fprintf(stderr, "Problem with reducers %d: %s", reducerID, strerror(errno));
    exit(-1);
  }

  if( (mid2 = msgget(keyID2, PERMS | IPC_CREAT)) == -1){
    fprintf(stderr, "Problem with reducers %d: %s", reducerID, strerror(errno));
    exit(-1);
  }

  //TODO receive data from the master
  if( (msgrcv(mid, (void *) &msg, sizeof(msg.msgText), mapperID, 0)) == -1){
    fprintf(stderr, "Problem receiving the data for reducers %d: %s", reducerID, strerror(errno));
    exit(-1);
  }

  //TODO check for END message and send ACK to master and then return 0
  //Otherwise return 1
  if(msg.msgText[0] == 4){
        memset(msg.msgText, '\0', MSGSIZE);
        msg.msgType = reducerID;

        if(msgsnd(mid2, &msg, MSGSIZE, 0) == -1){
          fprintf(stderr, "Problem with reducers %d: %s", reducerID, strerror(errno));
          exit(-1);
        }
        return 0;
    }else{
        strcpy(key, msg.msgText);
        return 1;
    }
  return 0;
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
