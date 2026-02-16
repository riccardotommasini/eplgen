package org.esper;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.EventPropertyDescriptor;
import com.espertech.esper.common.client.EventType;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.meta.EventTypeApplicationType;
import com.espertech.esper.common.client.meta.EventTypeMetadata;
import com.espertech.esper.common.client.module.Module;
import com.espertech.esper.common.client.module.ParseException;
import com.espertech.esper.common.client.util.EventTypeBusModifier;
import com.espertech.esper.common.internal.type.AnnotationTag;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class Parsing {

    public static void main(String[] args) throws IOException, ParseException, EPCompileException, EPDeployException, EPUndeployException {
        String query = args.length > 0 ? args[0] : "/epl_cases/ Q0001 .epl";

        File queryFile = new File(Path.of(query).toAbsolutePath().toString());

        Configuration config = new Configuration();
        config.getRuntime().getThreading().setInternalTimerEnabled(false);
        config.getCompiler().getByteCode().setAccessModifiersPublic();
        config.getCompiler().getByteCode().setBusModifierEventType(EventTypeBusModifier.BUS);

        ParsedModule parsed = parseAndRegister(queryFile, config);

        System.out.println("Validated: module parsed+compiled+deployed into Esper.");
        System.out.println("Schemas (MAP event types): " + parsed.eventSchemas.keySet());
        System.out.println("DML statements: " + parsed.dmlStatements.keySet());

        unregister(parsed);
    }

    public static final class ParsedModule {
        public final Configuration configuration;
        public final EPRuntime runtime;
        public final Module module;
        public final EPDeployment deployment;
        public final Map<String, EventPropertyDescriptor[]> eventSchemas;
        public final Map<String, EPStatement> dmlStatements;

        public ParsedModule(Configuration configuration,
                            EPRuntime runtime,
                            Module module,
                            EPDeployment deployment,
                            Map<String, EventPropertyDescriptor[]> eventSchemas,
                            Map<String, EPStatement> dmlStatements) {
            this.configuration = configuration;
            this.runtime = runtime;
            this.module = module;
            this.deployment = deployment;
            this.eventSchemas = eventSchemas;
            this.dmlStatements = dmlStatements;
        }
    }

    /**
     * Parse + compile + deploy into Esper runtime (validation gate).
     * No listeners, no event sending.
     */
    public static ParsedModule parseAndRegister(File queryFile, Configuration config)
            throws IOException, ParseException, EPCompileException, EPDeployException {

        EPCompiler compiler = EPCompilerProvider.getCompiler();
        EPRuntime esper = EPRuntimeProvider.getDefaultRuntime(config);
        esper.initialize();

        Module mod = compiler.readModule(queryFile);
        EPCompiled compiled = compiler.compile(mod, new CompilerArguments(config));
        EPDeployment deploy = esper.getDeploymentService().deploy(compiled);

        Map<String, EventPropertyDescriptor[]> eventSchemas = new HashMap<>();
        Map<String, EPStatement> dmlStatements = new HashMap<>();

        for (EPStatement st : deploy.getStatements()) {
            AnnotationTag tag = (AnnotationTag) st.getAnnotations()[0];

            if ("DML".equals(tag.value())) {
                dmlStatements.put(st.getName(), st);
            } else {
                EventType eventType = st.getEventType();
                if (eventType == null) continue;

                EventTypeMetadata meta = eventType.getMetadata();
                if (meta == null) continue;

                if (EventTypeApplicationType.MAP.equals(meta.getApplicationType())) {
                    eventSchemas.put(eventType.getName(), eventType.getPropertyDescriptors());
                }
            }
        }

        return new ParsedModule(config, esper, mod, deploy, eventSchemas, dmlStatements);
    }

    /**
     * Remove the deployment created during validation.
     */
    public static void unregister(ParsedModule parsed) throws EPDeployException, EPUndeployException {
        if (parsed == null || parsed.runtime == null || parsed.deployment == null) return;
        String id = parsed.deployment.getDeploymentId();
        if (id != null) {
            parsed.runtime.getDeploymentService().undeploy(id);
        }
    }
}
