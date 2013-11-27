import MapReduce
import sys

"""
Matrix Multiplication Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Application Usage :
#
#   python MatrixMultiply.py arg-1
#
#   arg-1 : File containg data for both the matrices in csv format.
#           Each row of the file represents one element of the matrix. 
#           Row Format:
#               matrix-name,row-number,column-number,cell-value
#               For ex: If A[1][4] = 9, 
#               the cell would be represented as: A,1,4,9
#
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#
# Algorithm :
#
# Key Idea 1: One reducer for each cell of the output matrix.
#             If dimensions of A and B are M x N and N x P respectively, there 
#             will be M*P reducers.
#
# Key Idea 2: Each element of the input matrices will be sent to multiple 
#             reducers (multiple keys will be generated).
#
# Mapper :
#   Each row of A should be sent to every column of the result.
#   => Element A[i,j] should be sent to every element in Result[i,k] 
#      where k = 0..P-1 where P is the number of columns of B.
#   Each column of B should be sent to every row of the result. 
#   => Element B[i,j] should be sent to every element in Result[k,j] 
#      where k = 0..M-1. M is the number of rows of A.
#  
#    
#             MAT A (3x4)                MAT B (4x2)
#         0     1     2     3             0     1   
#      -------------------------       -------------
#  0   | a1  |  a2 |  a3 |  a4 |       |  b1 |  b2 |
#      -------------------------       -------------
#  1   | a5  |  a6 |  a7 |  a8 |       |  b3 |  b4 |
#      -------------------------       -------------
#  2   | a9  | a10 | a11 | a12 |       |  b5 |  b6 |
#      -------------------------       -------------
#                                      |  b7 |  b8 |
#                                      -------------
#
#                             A x B (3 x 2)
#    (Mapping of elements from A)         (Mapping of elements from B)
#                0     1                           0     1 
#             -------------                     -------------
#          0  |  a1 |  a1 |                  0  | b1  |  b2 |  
#             |  a2 |  a2 |                     | b3  |  b4 |
#             |  a3 |  a3 |                     | b5  |  b6 |
#             |  a4 |  a4 |                     | b7  |  b8 |
#             -------------                     -------------
#          1  |  a5 |  a5 |                  1  | b1  |  b2 |
#             |  a6 |  a6 |                     | b3  |  b4 |
#             |  a7 |  a7 |                     | b5  |  b6 |
#             |  a8 |  a8 |                     | b7  |  b8 |
#             -------------                     -------------
#          2  |  a9 |  a9 |                  2  | b1  |  b2 |
#             | a10 | a10 |                     | b3  |  b4 |
#             | a11 | a11 |                     | b5  |  b6 |
#             | a12 | a12 |                     | b7  |  b8 |
#             -------------                     -------------
#
# Reducer:
#   Each reducer computes sum(A[i,j]*B[j,k]).
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
"""
  Questions:
  1. How would you handle sparse matrices? For a sparse matrix, you will not 
     have an entry in the input file for matrix elements that do not exist.
  2. The dimensions of the matrix are hardcoded into the application. How 
     will you provide this as input to the application?
     
  Extensions:
  1. What other matrix operations are amenable to the Map-Reduce approach?
"""
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def mapper(key,record):
    # Record Format : Record is a list of four elements in the foll format:
    #    [matrix, i, j, value] where,
    #          matrix is a string identifying the matrix ('A' or 'B' ).
    #          i, j are the row and column identifier of matrix cell. 
    #          value is the value of cell [i][j]
    # keys: 
    #    For matrix A, set of all (i,k) where k in 0..NUM_COLS_IN_B-1
    #    For matrix B, set of all (k,j) where j  in 0..NUM_ROWS_IN_A-1
    # Value: Full record as received.
    
    # The size of the o/p matrix is determined by the number of rows in A
    # and number of columns in B. Set these values. 
    NUM_ROWS_IN_A = 9
    NUM_COLS_IN_B = 3
    
    #Extract the name of the matrix
    matrixName = record[0]
    
    # The key is an array index (i,j). For matrix A, extract i from the
    # record. The values for j will be in the range 0..NUM_COLS_IN_B. You can 
    # use the range function for this. For matrix B, j will be picked from the
    # record and values of i will be in the range 0..NUM_ROWS_IN_A


def reducer(key, list_of_values):
    # key: Index of element in result matrix.
    # value: An element of matrix A or B in the following format:
    #        [matrix, i, j, value]
    
    # Initialize the value for the cell.
    total = 0
    
    # Extract records of 'A'
    matA = []
    for cell in list_of_values:
        if (cell[0] == "A"):
            matA.append(cell)
            
    # Extract records of 'B'
    
            
    # For each element from A, find all elements from B such that
    # the column number of A matches with the row number of B. (A[i,j]*B[j,k])
    # For each such pair, multiply the values of the cell and add 
    # it to a total.
    

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
if __name__ == '__main__':
  
  # Invoke the Map Reduce algorithm
  mr.execute(sys.argv[1], mapper, reducer,"CSV")