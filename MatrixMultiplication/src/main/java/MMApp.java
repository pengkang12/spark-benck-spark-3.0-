/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author PengKang
 */
package MatrixMultiplication.src.main.java;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function; 
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.SparkConf;
import org.apache.spark.rdd.RDD;
import java.util.ArrayList;
import java.util.List;

public class MMApp {

    public static void main(String[] args) {
        if (args.length < 5) {
            System.out.println("usage: <input> <output> <MatrixSize> <block> <numsplit> ");
            System.exit(0);
        }
        String input = args[0];
        String output = args[1];
        int MatrixSize = Integer.parseInt(args[2]);
        int block = Integer.parseInt(args[3]);
        int numsplit = Integer.parseInt(args[4]);
         
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);
        
        SparkConf conf = new SparkConf().setAppName("MMApp Example");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<MatrixEntry> matrixEntryList = new ArrayList<>();
 	for(int i=0; i<MatrixSize; i++)
       	    for(int j=0; j<MatrixSize; j++)
                matrixEntryList.add(new MatrixEntry(i, j, i*j));

        JavaRDD<MatrixEntry> matrixA = sc.parallelize(matrixEntryList, 1000);
        // a JavaRDD of (i, j, v) Matrix Entries

        int rowsPerBlock = block;
        int colsPerBlock = block;
        
        // Create a CoordinateMatrix from a JavaRDD<MatrixEntry>.
        CoordinateMatrix coordMat = new CoordinateMatrix(matrixA.rdd());
        // Transform the CoordinateMatrix to a BlockMatrix
        BlockMatrix matA = coordMat.toBlockMatrix(rowsPerBlock, colsPerBlock).cache();

        // Validate whether the BlockMatrix is set up properly. Throws an Exception when it is not valid.
        // Nothing happens if it is valid.
        matA.validate();

	long start = System.currentTimeMillis();
        // Calculate A^T A.
        BlockMatrix ata = matA.transpose().multiply(matA, numsplit);  
	double multiplicationTime = (double)(System.currentTimeMillis() - start) / 1000.0;
        //System.out.println("Matrix multiplication " + ata.blocks().toJavaRDD().collect());
         System.out.println("Matrix multiplication " + ata.numCols());
         System.out.println("Matrix multiplication " + ata.numRows());
        //System.out.printf("{\"loadTime\":%.3f,\"trainingTime\":%.3f,\"testTime\":%.3f,\"saveTime\":%.3f}\n", loadTime, trainingTime, testTime, saveTime);
        System.out.println("Matrix multiplication " + multiplicationTime);
        sc.stop();
    }
}
