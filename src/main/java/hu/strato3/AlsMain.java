package hu.strato3;

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
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

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
		
		MapContract mapper = MapContract.builder(new TokenizeLine()) .input(source).name("Tokenize Lines").build();
		ReduceContract reducer = ReduceContract.builder(InitQ.class, PactInteger.class, 0)
			.input(mapper)
			.name("Count Words")
			.build();
		
		FileDataSink out = new FileDataSink(new RecordOutputFormat(), output, mapper, "sink");
		FileDataSink out2 = new FileDataSink(new RecordOutputFormat(), output + "_q", reducer, "sink2");
		
		RecordOutputFormat.configureRecordFormat(out).recordDelimiter('\n').fieldDelimiter(',')
				.field(PactInteger.class, 0).field(PactInteger.class, 1).field(PactDouble.class, 2);
		
		ConfigBuilder config = RecordOutputFormat.configureRecordFormat(out2).recordDelimiter('\n').fieldDelimiter(',')
				.field(PactInteger.class, 0).field(PactDouble.class, 1);
		
		for (int i = 1; i < nFactors; i++) { config = config.field(PactDouble.class, i+1); }

		Plan plan = new Plan(out, "ALS Example");
		plan.setDefaultParallelism(numSubTasks);
		//return plan;
		return new Plan(new ArrayList<GenericDataSink>(Arrays.asList(new GenericDataSink[]{out,out2})));
	}

	public static class InitQ extends ReduceStub implements Serializable {
		private static final long serialVersionUID = 1L;
		private final PactRecord outputRecord = new PactRecord();
		
		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {
			PactRecord element = records.next();
			PactInteger i = element.getField(0, PactInteger.class);
			outputRecord.setField(0, i);
			for (int j = 0; j < nFactors; j++) {
				outputRecord.setField(j + 1, new PactDouble(1.0 * (j + 1) / nFactors));
			}
			out.collect(element);
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
