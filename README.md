# Project 2: IPC-based-Map-Reduce

- test machine: csel-kh1250-26
- Date: 03/20/2022
- name: Abdullahi Farah, Nathaniel Mckelvey
- X500: farah126, mckel042

## Project Purpose
The purpose of this project is to find the number of occurrences a given word may have within a text document. This action can take a while, but this project uses mapping/reducing methods that splits this task into multiple processes to run. This method increases speed at the cost of memory usage.

## How to Compile
The current structure of the Template folder should be maintained. If you want to add extra source(.c)
files, add it to src folder and for headers use #include. The current Makefile should be sufficient to execute
the code, but if you are adding extra files, modify the Makefile accordingly. For compiling the code, the
following steps should be taken:
```
cd Template
make
```

### Running the program
Once the make is successful, run the mapreduce code with the required mapper count, reducer count and
input file.
```
./mapreduce [#mappers] [#reducers] [path to input file]
```
example execution
```
./mapreduce 5 2 test/T1/F1.txt
```
The command `make t1` runs the mapreduce program with test input.

## Assumptions
- The template code will not error out on compiling.
- Number of mappers will be greater than or equal to number of reducers, other cases should error
out.
- The system calls that will be used for the project are fork, exec and wait.
- We assume the maximum size of a file path to be 50 bytes.
- The chunk size will be at most 1024 bytes as there is a chance that some of the 1024th byte
in input File is the middle of a word.
- The application will be ran in a linux environment replicating that used in the CSE Lab Machines

## Team Members
* Abdul Farah (farah126)
* Nathaniel McKelvey (mckel042)

## Contributions
### Abdul
- Implement the `sendChunkData()` function
- Implement the `getInterData()` function
- Added github actions to autobuild project
- Update Readme
   - How to compile the program
   - Any assumptions outside this document
   - Team member names and x500
   - Contribution by each member of the team

### Nathaniel
- Implemented `getchunkdata()` function
- Edited `sendchunkdata()` to work with `getchunkdata()`
- Update Nathaniel README contributions
