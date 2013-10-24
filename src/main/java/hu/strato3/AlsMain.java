package hu.strato3;

import java.io.Serializable;
import java.util.Iterator;

import eu.stratosphere.pact.client.PlanExecutor;
import eu.stratosphere.pact.client.RemoteExecutor;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.example.util.AsciiUtils;


public class AlsMain  implements PlanAssembler, PlanAssemblerDescription {
	

  public static class TokenizeLine extends MapStub implements Serializable {
		private static final long serialVersionUID = 1L;
		
		// initialize reusable mutable objects
		private final PactRecord outputRecord = new PactRecord();
		private final PactInteger first = new PactInteger(0);
		private final PactInteger second = new PactInteger(0);
		private final PactDouble value = new PactDouble(0.0);
		
		private final AsciiUtils.WhitespaceTokenizer tokenizer =
				new AsciiUtils.WhitespaceTokenizer();
		
		@Override
		public void map(PactRecord record, Collector<PactRecord> collector) {
			// get the first field (as type PactString) from the record
			PactString line = record.getField(0, PactString.class);
			String[] splitted = line.getString().split("\\|");
                        first.setInteger(Integer.parseInt(splitted[0]));
                        second.setInteger(Integer.parseInt(splitted[1]));
                        value.setDouble(Double.parseDouble(splitted[2]));
			this.outputRecord.setField(0, this.first);
			this.outputRecord.setField(1, this.second);
			this.outputRecord.setField(2, this.value);
			collector.collect(this.outputRecord);
		}
	}


@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		int numSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataInput = (args.length > 1 ? args[1] : "");
		String output    = (args.length > 2 ? args[2] : "");

		FileDataSource source = new FileDataSource(new TextInputFormat(), dataInput, "Input Lines");
		source.setParameter(TextInputFormat.CHARSET_NAME, "ASCII");		// comment out this line for UTF-8 inputs
		MapContract mapper = MapContract.builder(new TokenizeLine())
			.input(source)
			.name("Tokenize Lines")
			.build();
                /*
		ReduceContract reducer = ReduceContract.builder(CountWords.class, PactString.class, 0)
			.input(mapper)
			.name("Count Words")
			.build();
                        */
		FileDataSink out = new FileDataSink(new RecordOutputFormat(), output, mapper, "sink");
		RecordOutputFormat.configureRecordFormat(out)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(PactString.class, 0)
			.field(PactInteger.class, 1);
		
		Plan plan = new Plan(out, "ALS Example");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [numSubStasks] [input] [output]";
	}
	
	public static void main(String[] args) throws Exception {
		AlsMain als = new AlsMain();
		Plan plan = als.getPlan(args[0], args[1], args[2]);
		// This will create an executor to run the plan on a cluster. We assume
		// that the JobManager is running on the local machine on the default
		// port. Change this according to your configuration.
		PlanExecutor ex = new RemoteExecutor("localhost", 6123, "target/pact-examples-0.4-SNAPSHOT-WordCount.jar");
		ex.executePlan(plan);
	}
}
