/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author PengKang
 */
package MatrixMultiplication.src.main.java;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function; 
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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
        
        SparkConf conf = new SparkConf().setAppName("MMApp CooridnateMatrix");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //List<MatrixEntry> matrixEntryList = new ArrayList<>();
 	//for(int i=0; i<MatrixSize; i++)
       	//    for(int j=0; j<MatrixSize; j++)
                //matrixEntryList.add(new MatrixEntry(i, j, i*j));
        List<Integer> entryList = new ArrayList<>();
 	for(int i=0; i<MatrixSize; i++)
                entryList.add(i);
 
        JavaRDD rdd = sc.parallelize(entryList);

        JavaPairRDD<Integer, Integer> pairs = rdd.cartesian(rdd);
        JavaRDD<MatrixEntry> entries = pairs.map(tup ->
                new MatrixEntry(tup._1(), tup._2(), tup._1() ));
        
        //JavaRDD<MatrixEntry> matrixA = sc.parallelize(matrixEntryList, 200);
        // a JavaRDD of (i, j, v) Matrix Entries

        int rowsPerBlock = block;
        int colsPerBlock = block;
        
        // Create a CoordinateMatrix from a JavaRDD<MatrixEntry>.
        CoordinateMatrix coordMat1 = new CoordinateMatrix(entries.rdd());
        CoordinateMatrix coordMat2 = new CoordinateMatrix(entries.rdd());
 
        // Transform the CoordinateMatrix to a BlockMatrix
        BlockMatrix matA = coordMat1.toBlockMatrix(rowsPerBlock, colsPerBlock);
        BlockMatrix matB = coordMat2.toBlockMatrix(rowsPerBlock, colsPerBlock);

	long start = System.currentTimeMillis();
        // Calculate A^T A.
        BlockMatrix ata = matA.multiply(matB, numsplit);  
	double multiplicationTime = (double)(System.currentTimeMillis() - start) / 1000.0;
        //System.out.println("Matrix multiplication " + ata.blocks().toJavaRDD().collect());
         System.out.println("Matrix multiplication " + ata.numCols());
         System.out.println("Matrix multiplication " + ata.numRows());
        //System.out.printf("{\"loadTime\":%.3f,\"trainingTime\":%.3f,\"testTime\":%.3f,\"saveTime\":%.3f}\n", loadTime, trainingTime, testTime, saveTime);
        System.out.println("Matrix multiplication " + multiplicationTime);
        sc.stop();
    }
}
