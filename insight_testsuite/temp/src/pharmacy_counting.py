"""
Created on Sun Jul 15 18:16:04 2018

@author: Fatemeh Mokhtari (email:f.mokhtari79@gmail.com)
"""
#%% Import 'sys' library to get input and output file names from the shell script
import sys

#%%************************* Process a chunk of data **************************
def ProcessChunk(d):
    '''This function gets the list of the lines of a chunk of the data, and 
    returns a dictionary, where the keys of dictionary are drug names, and 
    the values of dictionary are a list of unique prescribers, number of 
    prescriptions and total cost for each drug.
    Input:
        d: the list of the lines of a chunk of the data
    Output:
        drugDict: the drug dictionary with the keys and values as explained 
        above'''
    
    #%% Initilize the dictionary of drugs for each chunk, 
    drugDict = {}
    
    for item in d:
        line = item.split(',')
        #%% Clean the data: checks if the length of each line of the input is 5
        # and there is no empty field in the line
        if (len(line)==5) and ('' not in line):
            #%% Drug is not in the drugDict dictionary, define a new key and 
            # set the values            
            if line[3] not in drugDict:                
                try: # This try/except actually checks if the cost of a line is
                     # a string of numerical characters. Thus, it skips the 
                     # lines for which the cost includes any special or 
                     # alphabetic characters  
                    drugDict[line[3]] = {'prescriber' : [line[1] + ' ' + line[2]],\
                        'num_prescriber':1,'total_cost': float(line[4])}
                except:
                    continue
            #%% Drug is in the drugDict dictionary
            elif line[3] in drugDict.keys():
                #%% Prescriber is new, update the list of prescribers, number 
                # of prescriptions and total cost of the drug
                if line[1] + ' ' + line[2] not in drugDict[line[3]]['prescriber']:
                    try: # see the comment of the try/except above 
                        drugDict[line[3]]['prescriber'].append(line[1] + ' ' + line[2])                 
                        drugDict[line[3]]['num_prescriber'] += 1
                        drugDict[line[3]]['total_cost'] += float(line[4])
                    except:
                        continue 
                #%% Prescriber is not new, only update the total cost 
                else:
                    try: # see the comment of the try/except above
                        drugDict[line[3]]['total_cost'] += float(line[4]) 
                    except:
                        continue
    return drugDict


#%% ************************** Read a chunk of data ***************************
def ReadLines(fHandle,N):
    '''This function gets the text file handle, and chunk size, and returns a list 
    of the lines that fall within the current chunk.
    Input:
        fHandle: text file handle
        N: the chunk size
    Output: 
        Lines: the list of the lines within the chunk'''
    Lines = []

    for n in range(N):
        Lines.append(fHandle.readline())
    
    return Lines


        
#%%****************************************************************************
#******************************** Main code ***********************************
    
def main():
    
    #%% Get input and output file name from the shell script.
    InputFile   = sys.argv[1]
    OutputFile  = sys.argv[2]
    #%% Set maximum size of one chunk     
    Onechunk    = 1e5       # maximum size of one chunk
    
    #%% Initialize the dictionary of total data
    totalDict    = {}
    
    #%% Calculate number of chunks and size of each chunk
    NumLines   = sum(1 for line in open(InputFile))-1
    ChunkSize  = [int(Onechunk)]*int(NumLines/Onechunk)
    
    if sum(ChunkSize) < NumLines:
        ChunkSize.append(int(NumLines-sum(ChunkSize)))
    
    #%% Open file handle and skip the first line which is the header line    
    fHandle = open(InputFile)
    fHandle.readline();
    
    
    #%% Read each chunk and process the data
    for n in range(len(ChunkSize)):
        chunkDict = ProcessChunk(ReadLines(fHandle,ChunkSize[n]))
        
        #%% Merge the output dictionary from each chunk (named chunkDict) 
        # with the dictionary of total data (named totalDict) 
        if len(totalDict):
            for key,val in chunkDict.items():
                if key not in totalDict:
                    totalDict[key] = val
                else:
                    totalDict[key]['total_cost'] += val['total_cost']
                    for P in val['prescriber']:
                        if P not in totalDict[key]['prescriber']:
                            totalDict[key]['num_prescriber'] += 1
                            totalDict[key]['prescriber'].append(P)
        else:
            totalDict = chunkDict
        print('Chunk number: ' + str(n+1))
        
        
    #%% Sort the dictionary data based on the total cost in descending order, and 
    # then if there is a tie in total cost, sort based on the drug name in 
    # ascending order
    sortedDict = sorted(totalDict.items(), key=lambda d: (-d[1]['total_cost'], d[0])) 

    drugList =[]
    for item in sortedDict:
        drugList.append(item)
 
    #%% Write data into a text file named 'top_cost_drug.txt'    
    out = open(OutputFile, "w")
    out.write('drug_name,num_prescriber,total_cost\n')
    for item in drugList:
        out.write(item[0] + ',' + str(item[1]['num_prescriber']) + ',' + str(item[1]['total_cost']) + '\n')
    out.close()   
    
if __name__ == '__main__':
   main()