import MapReduce
import sys

"""
This application is a map reduce implementation to implement
SQL operations on a single table
"""

mr = MapReduce.MapReduce()

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# 
#  Application Usage:
#
#      python SQLSelect.py arg-1 
#
#      arg-1 : File name containing table data in csv format. 
#              The first row of the file should contain the column names. 
#              The first column of each row should identify the table name. 
#  The application is hard-coded to implement a SQL statement that answers
#  the following question on the "Posts" data of stack overflow:
#
#  Are there any questions(posts) that do not have an answer as yet? If yes, 
#  list the Title, Score, ViewCount and CommentsCount of the post. 
#  
#  The SQL statement that would answer the above question would be: 
#  SELECT Title, Score, ViewCount, CommentsCount
#  FROM Posts
#  WHERE AnswerCount = 0
#  
#  Dataset : ..\datasets\cstheory_sample\Posts_Sample.csv
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#  
#  Algorithm Design:
#  
#  Key Idea: Send all records that match the predicate in the WHERE clause
#            to the same reducer.
#
#  Initial Setup:
#  1. Read the first line of the input file and store the column names 
#     as a dictionary. The key will be the column name and the value will
#     be it's position in the row.
#     This should be done before calling the execute method of MapReduce
#     The MapReduce module is designed to skip the first line of a CSV file.
# 
#  Mapper:
#  1. Input:  One line from the file. This will be a list containing the
#             values of each column of a record.
#  2. Key   : AnswerCount
#     value : (Title, Score, ViewCount, CommentsCount)
#     Do not emit anything if the AnswerCount field is NULL. 
#  Reducer:
#     Emit an output only if the key is zero.     
#
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++       

"""
  Questions:
  1. How would you extend the algorithm to include questions that have answers 
     no accepted answer?
     
  Extensions:
  1. Extend the application to process statement written in SQL syntax. The SQL 
     statement will be provided in an input file as an argument to the application. 
     The table names can be replaced with actual file names as shown below:
     <SQL>
     <Table name="Posts", file="Posts_Sample.xml", path="..\Datasets\CSTheory_Sample"/>
     SELECT OwnerUserId
     FROM Posts
     WHERE Score > 10
     </SQL>
     The column names should be treated in a case-insensitive manner.
"""
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Global list to hold column names
columnNames = {}

# Record Format : A line from the input file.
def mapper(key,record):
        
    global columnNames
    
    # Get the column index for column AnswerCount from the columnNames dict.
    answerCountIdx = columnNames['AnswerCount']
    
    # Extract the value of AnswerCount from the record. This will be the key 
    # emitted by the mapper. It is possible that the key is null if there 
    # is no value for the AnswerCount field.
    answerCount = record[answerCountIdx]
    
    # Get the column index for Title, Score, ViewCount and CommentsCount
    
    
    # Setup the value as a tuple "val = (a,b,c) "
    value = ('''Fill in the values here''')
    
    # Emit only if answerCount is not NULL
    

def reducer(key, list_of_values):
     
    # Emit output only if key is zero. The key will be a string
    

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def main():
  # Extract the first line from the file to get the column names.
  tableData = open(sys.argv[1])
  firstLine = tableData.readline()
  tableData.close()
  
  # remove the newline at the end of the line
  firstLine = firstLine.replace('\n','')
  
  # Extract the list of column names
  columnNamesList = firstLine.split(",")
  
  # Assign the column index to each column.
  colIndex = 0
  for columnName in columnNamesList:
    columnNames[columnName] = colIndex
    colIndex += 1
  
  # Call map reduce.
  inputdata = open(sys.argv[1])
  fileNameList = []
  fileNameList.append(sys.argv[1])
  mr.execute(fileNameList, mapper, reducer,"CSV-SkipFirstLine")

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++  
if __name__ == '__main__':
    main()