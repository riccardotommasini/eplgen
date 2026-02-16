package org.esper;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.common.client.EventPropertyDescriptor;
import com.espertech.esper.common.client.EventType;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.meta.EventTypeApplicationType;
import com.espertech.esper.common.client.meta.EventTypeMetadata;
import com.espertech.esper.common.client.module.Module;
import com.espertech.esper.common.client.module.ParseException;
import com.espertech.esper.common.client.util.EventTypeBusModifier;
import com.espertech.esper.common.client.util.SafeIterator;
import com.espertech.esper.common.internal.event.arr.ObjectArrayEventType;
import com.espertech.esper.common.internal.type.AnnotationTag;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Running {

    public static void main(String[] args) throws IOException, ParseException, EPCompileException, EPDeployException, InterruptedException {

        String query = args.length > 0 ? args[0] : "/epl_cases/Q0001";

        String stream_file = query + ".csv";
        String query_file = query + ".epl";

        String statementId = query + "_Original;" +
                             query + "_Decomp_Final";

        String out_directory = query + "/out/";

        File queryFile = new File(query_file);
        File streamFile = new File(stream_file);
        File outDir = new File(out_directory);

        Configuration config = new Configuration();
        run(outDir, streamFile, queryFile, statementId, config);

    }


    private static void run(File outDir, File streamFile, File queryFile, String statementId, Configuration config) throws IOException, ParseException, EPCompileException, EPDeployException, InterruptedException {

        config.getRuntime().getThreading().setInternalTimerEnabled(false);
        config.getCompiler().getByteCode().setAccessModifiersPublic();
        config.getCompiler().getByteCode().setBusModifierEventType(EventTypeBusModifier.BUS);
        EPCompiler compiler = EPCompilerProvider.getCompiler();
        EPRuntime esper = EPRuntimeProvider.getDefaultRuntime(config);

        esper.initialize();
        esper.getEventService().advanceTime(0);


        Module mod = compiler.readModule(queryFile);

        CompilerArguments compilerArguments = new CompilerArguments(config);
        EPCompiled compiled = compiler.compile(mod, compilerArguments);

        EPDeployment deploy = esper.getDeploymentService().deploy(compiled);

        List<Thread> threadList = new ArrayList<>();

        Map<String, EventPropertyDescriptor[]> eventSchemas = new HashMap<>();

        for (EPStatement statement : deploy.getStatements()) {
            AnnotationTag annotation = (AnnotationTag) statement.getAnnotations()[0];
            if (annotation.value().equals("DML")) {
                if (statementId.contains(statement.getName())) {
                    if (statement.getName().contains("Pull")) {
                        threadList.add(new Thread(() -> {
                            while (true) {
                                pullTable(statement);
                            }
                        }));
                    } else {
                        statement.addListener(new FileLogListener(outDir));
                    }
                }
            } else {
                //Reading input data file
                EventType eventType = statement.getEventType();
                EventPropertyDescriptor[] propertyDescriptors = eventType.getPropertyDescriptors();
                EventTypeMetadata metadata = eventType.getMetadata();
                EventTypeApplicationType applicationType = metadata.getApplicationType();
                if (applicationType.equals(EventTypeApplicationType.MAP)) {
                    eventSchemas.put(eventType.getName(), propertyDescriptors);
                }
            }
        }

        threadList.add(new Thread(() -> {


            try {

                FileReader in = new FileReader(streamFile);
                ;
                BufferedReader bufferedReader = new BufferedReader(in);
                // Reading the first line of the file for the event schema

                String header = bufferedReader.readLine();
                String e = bufferedReader.readLine();

                while (e != null) {
                    String[] data = e.split(",");//;e.replace("[", "").replace("]", "").split(",");
                    long nextTime = Long.parseLong(data[1].trim());
                    long currentTime = esper.getEventService().getCurrentTime();

                    String type = data[0].trim();
                    if (!eventSchemas.containsKey(type))
                        type = (String) eventSchemas.keySet().toArray()[0];
                    //to avoid excessive dealy, we uniform the streams

                    EventPropertyDescriptor[] propertyDescriptors = eventSchemas.get(type);

                    Map<String, Object> event = new HashMap<>();

                    for (int i = 0; i < propertyDescriptors.length && i < data.length - 2; i++) {
//                        EventType,Timestamp,camera,therm,temp,humid,x,y,sensor
                        Object value = data[i + 2].trim();
                        if (Long.class.equals(propertyDescriptors[i].getPropertyType())) {
                            value = Long.parseLong(value.toString());
                        } else if (Integer.class.equals(propertyDescriptors[i].getPropertyType())) {
                            value = Integer.parseInt(value.toString());
                        } else if (Double.class.equals(propertyDescriptors[i].getPropertyType())) {
                            value = Double.parseDouble(value.toString());
                        } else {
                            value = value.toString();
                        }
                        event.put(propertyDescriptors[i].getPropertyName(), value);
                    }

                    if (currentTime > nextTime) continue; //out of order
                    else if (currentTime < nextTime) {
//                        System.err.println("Got an [" + data[0].trim() + "] at [" + nextTime + "]");
                        esper.getEventService().advanceTime(nextTime);
                    }

                    esper.getEventService().sendEventMap(event, type);

                    e = bufferedReader.readLine();

                    //Create a realistic pace for the execution
                    Thread.sleep(nextTime - currentTime);

                }
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }));

        threadList.forEach(Thread::start);

    }

    private static void pullTable(EPStatement statement) {
        SafeIterator<EventBean> eventBeanSafeIterator = statement.safeIterator();
        while (eventBeanSafeIterator.hasNext()) {
            EventBean next = eventBeanSafeIterator.next();
            if (next.getEventType() instanceof ObjectArrayEventType) {
                String[] propertyNames = next.getEventType().getPropertyNames();
                for (String pn : propertyNames) {
                    System.out.println(pn + "=" + next.get(pn));
                }
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }


}