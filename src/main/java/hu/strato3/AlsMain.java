package hu.strato3;

import Jama.Matrix;
import java.io.Serializable;

import eu.stratosphere.pact.client.PlanExecutor;
import eu.stratosphere.pact.client.RemoteExecutor;
import eu.stratosphere.pact.common.contract.*;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat.ConfigBuilder;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

public class AlsMain implements PlanAssembler, PlanAssemblerDescription {

	private static final int nFactors = 10;
	
	public static class TokenizeLine extends MapStub implements Serializable {
		private static final long serialVersionUID = 1L;

		// initialize reusable mutable objects
		private final PactRecord outputRecord = new PactRecord();
		private final PactInteger first = new PactInteger(0);
		private final PactInteger second = new PactInteger(0);
		private final PactDouble value = new PactDouble(0.0);

		@Override
		public void map(PactRecord record, Collector<PactRecord> collector) {
			// get the first field (as type PactString) from the record
			PactString line = record.getField(0, PactString.class);
			String[] splitted = line.getValue().split("\\|");
			first.setValue(Integer.parseInt(splitted[0]));
			second.setValue(Integer.parseInt(splitted[1]));
			value.setValue(Double.parseDouble(splitted[2]));
			this.outputRecord.setField(0, this.first);
			this.outputRecord.setField(1, this.second);
			this.outputRecord.setField(2, this.value);
			collector.collect(this.outputRecord);
		}
	}

	public static class TokenizeLineFactors extends MapStub implements Serializable {
		private static final long serialVersionUID = 1L;

		// initialize reusable mutable objects
		private final PactRecord outputRecord = new PactRecord();

		@Override
		public void map(PactRecord record, Collector<PactRecord> collector) {
			// get the first field (as type PactString) from the record
			PactString line = record.getField(0, PactString.class);
			String[] splitted = line.getValue().split("\\|");
			
			int id = Integer.parseInt(splitted[0]);
			this.outputRecord.setField(0, new PactInteger(id));
			for (int i = 1; i < splitted.length; i++) {
				double value = Double.parseDouble(splitted[i]);
				this.outputRecord.setField(i, new PactDouble(value));
			}
			collector.collect(this.outputRecord);
		}
	}	
	
	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataInput = (args.length > 1 ? args[1] : "");
		String output = (args.length > 2 ? args[2] : "");

		FileDataSource source = new FileDataSource(new TextInputFormat(), dataInput, "Input Lines");
		source.setParameter(TextInputFormat.CHARSET_NAME, "ASCII");
		
		MapContract ratingsInput = MapContract.builder(new TokenizeLine()).input(source).name("Tokenize Lines").build();
		ReduceContract factorsInput = ReduceContract.builder(InitQ.class, PactInteger.class, 0)
			.input(ratingsInput)
			.name("Count Words")
			.build();
		
		MatchContract match = MatchContract.builder(UserItemRatingFactorMatch.class, PactInteger.class, 0, 0)
				.input1(ratingsInput).input2(factorsInput).name("User-item-rating factors match").build();
		
		ReduceContract solve = ReduceContract.builder(ComputeP.class, PactInteger.class, 1 /* EZ itt a user id kulcsa!!! */) 
				.input(match).name("LS solve").build();
		
		FileDataSink out = new FileDataSink(new RecordOutputFormat(), output, ratingsInput, "sink");
		FileDataSink out2 = new FileDataSink(new RecordOutputFormat(), output + "_q", factorsInput, "sink2");
		FileDataSink out3 = new FileDataSink(new RecordOutputFormat(), output + "_match", match, "sink3");
		FileDataSink out4 = new FileDataSink(new RecordOutputFormat(), output + "_solve", solve, "sink4");
		
		RecordOutputFormat.configureRecordFormat(out).recordDelimiter('\n').fieldDelimiter(',')
				.field(PactInteger.class, 0).field(PactInteger.class, 1).field(PactDouble.class, 2);
		
		ConfigBuilder config2 = RecordOutputFormat.configureRecordFormat(out2).recordDelimiter('\n').fieldDelimiter(',')
				.field(PactInteger.class, 0);
		for (int i = 0; i < nFactors; i++) { config2 = config2.field(PactDouble.class, i+1); }

		ConfigBuilder config3 = RecordOutputFormat.configureRecordFormat(out3).recordDelimiter('\n').fieldDelimiter(',')
				.field(PactInteger.class, 0).field(PactInteger.class, 1).field(PactDouble.class, 2);
		for (int i = 0; i < nFactors; i++) { config3 = config3.field(PactDouble.class, i + 3); }	

		ConfigBuilder config4 = RecordOutputFormat.configureRecordFormat(out4).recordDelimiter('\n').fieldDelimiter(',')
				.field(PactInteger.class, 0);
		for (int i = 0; i < nFactors; i++) { config4 = config4.field(PactDouble.class, i+1); }		
		
		Plan plan = new Plan(out, "ALS Example");
		plan.setDefaultParallelism(numSubTasks);
		//return plan;
		return new Plan(new ArrayList<GenericDataSink>(Arrays.asList(new GenericDataSink[]{out,out2,out3,out4})));
	}

	public static class InitQ extends ReduceStub implements Serializable {
		private static final long serialVersionUID = 1L;
		private final PactRecord outputRecord = new PactRecord();
		
		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {
			PactRecord element = records.next();
			PactInteger i = element.getField(0, PactInteger.class);
			outputRecord.setField(0, i);
			Random r = new Random();
			for (int j = 0; j < nFactors; j++) {
				//outputRecord.setField(j + 1, new PactDouble(1.0 * (j + 1) / nFactors));
				outputRecord.setField(j + 1, new PactDouble(r.nextDouble()));
			}
			out.collect(outputRecord);
		}
	}
	
	public static class ComputeP extends ReduceStub implements Serializable {
		private static final long serialVersionUID = 1L;
		private final PactRecord outputRecord = new PactRecord();
		private static final double lambda = 0.1;
		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {
			
			double[][] QQ = new double[nFactors][nFactors];
			double[] outQ = new double[nFactors];
			
			int userId = -1;
			int nEvents = 0;
			while(records.hasNext()) {
				PactRecord record = records.next();
				if (userId < 0) { userId = record.getField(1, PactInteger.class).getValue(); }
				double r = record.getField(2, PactDouble.class).getValue();
				double[] qi = new double[nFactors];
				for (int k = 0; k < qi.length; k++) { qi[k] = record.getField(k + 3, PactDouble.class).getValue(); }
				Util.incrementMatrix(QQ, qi);
				Util.incrementVector(outQ, qi, r);
				nEvents++;
			}
			if (userId < 0) { throw new RuntimeException("Unknown user id."); }
			Util.fillLowerMatrix(QQ);
			Util.addRegularization(QQ, (nEvents + 1) * lambda);
			System.out.println("-------------------------------------");
			System.out.println("UserId");
			System.out.println("Matrix to invert:\n" + Util.getMatrixString(QQ));
			System.out.println("Out vector:\n" + Util.getVectorString(outQ));
			
			Matrix matrix = new Matrix(QQ);
			Matrix rhs = new Matrix(outQ, outQ.length);
			Matrix pu = matrix.chol().solve(rhs);
			
			outputRecord.setField(0, new PactInteger(userId));
			for (int i = 0; i < nFactors; i++) {
				outputRecord.setField(i + 1, new PactDouble(pu.get(i, 0)));
			}
			System.out.println("pu:\n" + Util.getMatrixString(Util.toDoubleArrayArray(pu)));
			System.out.println("-------------------------------------");
			out.collect(outputRecord);
		}
	}
	
	public static class UserItemRatingFactorMatch extends MatchStub implements Serializable {

		@Override
		public void match(PactRecord ratings, PactRecord factors, Collector<PactRecord> out) throws Exception {
			PactRecord res = new PactRecord(nFactors + 3);
			res.setField(0, ratings.getField(0, PactInteger.class));
			res.setField(1, ratings.getField(1, PactInteger.class));
			res.setField(2, ratings.getField(2, PactDouble.class));
			for (int i = 0; i < nFactors; i++) { res.setField(i + 3, factors.getField(i + 1, PactDouble.class)); }
			out.collect(res);
		}
	}

	@Override
	public String getDescription() {
		return "Parameters: [numSubStasks] [input] [output]";
	}
	
	public static void main(String[] args) throws Exception {
		AlsMain als = new AlsMain();
		Plan plan = als.getPlan(args[0], args[1], args[2]);
		//Plan plan = als.getPlan(args[0], args[1], args[2], args[3]);
		// This will create an executor to run the plan on a cluster. We assume
		// that the JobManager is running on the local machine on the default
		// port. Change this according to your configuration.
		PlanExecutor ex = new RemoteExecutor("localhost", 6123, "target/pact-examples-0.4-SNAPSHOT-WordCount.jar");
		ex.executePlan(plan);
	}
}
