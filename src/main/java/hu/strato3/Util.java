/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hu.strato3;

import Jama.Matrix;

/**
 *
 * @author DaveXster
 */
public class Util {

	public static void incrementMatrix(double[][] M, double[] v) {
		for (int i = 0; i < v.length; i++) {
			double[] mi = M[i];
			double vi = v[i];
			for (int j = i; j < v.length; j++) { mi[j] += vi * v[j]; }
		}
	}
	
	public static void incrementVector(double[] out, double[] v, double r) {
		for (int i = 0; i < v.length; i++) { out[i] += v[i] * r; }
	}
	
	public static void fillLowerMatrix(double[][] M) {
		for (int i = 0; i < M.length; i++) {
			for (int j = i; j < M.length; j++) {
				M[j][i] = M[i][j];
			}
		}
	}
	
	public static void addRegularization(double[][] M, double r) {
		for (int i = 0; i < M.length; i++) { M[i][i] += r; }
	}
	
	public static String getMatrixString(double[][] M) {
		String res = "";
		for (int i = 0; i < M.length; i++) {
			for (int j = 0; j < M.length; j++) {
				res += String.format("[%.6f]", M[i][j]);
			}
			res += "\n";
		}
		return res;
	}
	public static String getVectorString(double[] v) {
		String res = "";
		for (int i = 0; i < v.length; i++) {
			res += String.format("[%.6f]", v[i]);
		}
		return res;
	}
}
