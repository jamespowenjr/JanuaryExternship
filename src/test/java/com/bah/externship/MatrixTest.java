package com.bah.externship;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.junit.Test;

public class MatrixTest {

    @Test
    public void testMatrixMult() {
        // 2 x 3 matrix
        double[][] matrix1 = new double[][]{ {4, 6, 8}, {5, 7, 9} };
        // 3 x 2 matrix
        double[][] matrix2 = new double[][]{ {1, 3}, {6, 7}, {4, 1} };

        RealMatrix m1 = MatrixUtils.createRealMatrix(matrix1);
        RealMatrix m2 = MatrixUtils.createRealMatrix(matrix2);

        RealMatrix resultMatrix = m1.multiply(m2);

        System.out.println(matrixToString(resultMatrix.getData()));
    }

    // Convenience method for outputing a matrix to a String
    private String matrixToString(double[][] matrix){
        String output = "";
        for(int r = 0; r < matrix.length; r++){
            for(int s = 0; s < matrix[r].length; s++){
                output += matrix[r][s];
                if(s < matrix[r].length - 1){
                    output += "\t";
                }
            }
            output += "\n";
        }
        return output;
    }
}
