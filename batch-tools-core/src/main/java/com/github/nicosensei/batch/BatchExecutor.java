/**
 *
 */
package com.github.nicosensei.batch;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.github.nicosensei.commons.utils.FileUtils;
import com.github.nicosensei.commons.utils.datatype.TimeFormatter;


/**
 *
 * Generic batch executors that wraps basic services such as property injection and logging.
 * @author ngiraud
 *
 */
public final class BatchExecutor {

    private class BatchSettings extends Properties {

        private static final long serialVersionUID = 4321353542724244541L;
        
        public static final String BASE_PACKAGE_PROP_KEY = "basePackage";
        
        private final String basePackage; 

        private BatchSettings(String filePath) 
        		throws InvalidPropertiesFormatException, IOException {
            FileInputStream fis = new FileInputStream(filePath);
            loadFromXML(fis);
            
            basePackage = getProperty(BASE_PACKAGE_PROP_KEY);
            if (basePackage == null) {
            	throw new InvalidPropertiesFormatException(
            			"Must define a " + BASE_PACKAGE_PROP_KEY + " property!");
            }
        }

        public String getPropertyKey(Class<?> owner, String name) {
        	String prefix = owner.getName();
        	if (!basePackage.isEmpty()) {
        		prefix = prefix.replaceFirst(basePackage + "\\.", "");
        	}
            return prefix + "." + name;
        }

    }

    private class BatchLogger {

        private static final String DFT_LOG_PATTERN = "%m%n";
        
        private static final String MAIN_LOG_EXT = ".out";
        private static final String ERROR_LOG_EXT = ".err";

        public final String logPattern;

        private Logger mainLogger;
        private String mainLogFileName;
        private Logger errorLogger;
        private String errorLogFileName;
        
        private final File logFolder;

        private BatchLogger(
                BatchSettings settings,
                String name,
                String dateLayout,
                String logPattern) throws IOException {

            if (logPattern == null || logPattern.isEmpty()) {
                this.logPattern = DFT_LOG_PATTERN;
            } else {
                this.logPattern = logPattern;
            }

            this.logFolder = new File(getProperty(BatchExecutor.class, "log.folder"));

            if (! this.logFolder.exists()) {
                this.logFolder.mkdir();
            } else if (! this.logFolder.isDirectory()
                    || ! this.logFolder.canWrite()) {
                System.err.println("Can't initialize log folder!");
                System.exit(-1);
            }

            Logger rootLogger = Logger.getRootLogger();
            if (! rootLogger.getAllAppenders().hasMoreElements()) {
                rootLogger.addAppender(new ConsoleAppender(
                        new PatternLayout(this.logPattern)));
            }
            rootLogger.setLevel(Level.ERROR);
            rootLogger.setAdditivity(false);

            String baseLogFileName = logFolder.getAbsolutePath()
            		+ File.separator + name 
            		+ "_" + new SimpleDateFormat(dateLayout).format(new Date());
            
            mainLogger = Logger.getLogger(name);
            mainLogger.addAppender(
                    new ConsoleAppender(new PatternLayout(this.logPattern)));
            this.mainLogFileName = baseLogFileName + MAIN_LOG_EXT;
            mainLogger.addAppender(
                    new FileAppender(new PatternLayout(this.logPattern), mainLogFileName));
            mainLogger.setLevel(Level.DEBUG);
            mainLogger.setAdditivity(false);            

            errorLogger = Logger.getLogger(name + ".err");
            this.errorLogFileName = baseLogFileName + ERROR_LOG_EXT;
            errorLogger.addAppender(
                    new FileAppender(new PatternLayout(this.logPattern), errorLogFileName));
            errorLogger.setLevel(Level.ERROR);
            errorLogger.setAdditivity(false);
            
            mainLogger.info("Log folder is: " + logFolder.getAbsolutePath());
            mainLogger.info("Console output logged to: " + mainLogFileName);
            mainLogger.info("Errors logged to: " + errorLogFileName);
        }

    }

    private static BatchExecutor instance;

    private BatchSettings settings;
    private BatchLogger logger;
    private List<File> filesToClean = new ArrayList<File>();
    
    private BatchExecutor(String settingsFilePath, String name)
    throws InvalidPropertiesFormatException, IOException {
        settings = new BatchSettings(settingsFilePath);
        logger = new BatchLogger(
                settings,
                name,
                getProperty(BatchExecutor.class, "log.dateLayout"),
                getProperty(BatchExecutor.class, "log.pattern"));
    }

    public static void launch(
    		final String settingsXmlPath, 
    		final Class<?> batchClass,
    		final String... args) {

        long initTime = System.currentTimeMillis();
        try {
            BatchExecutor.init(settingsXmlPath, batchClass.getSimpleName());
        } catch (Exception e) {
            e.printStackTrace(System.err);
            System.err.println("Failed to initialize batch!");
            System.exit(-1);
        }

        BatchState state = null;
        try {
            Batch<?, ?> batch = (Batch<?, ?>) batchClass.newInstance();

            batch.initialize(args);
            batch.launch();

            state = batch.getBatchState();
        } catch (BatchException e) {
            if (Level.FATAL.equals(e.getCriticity())) {
                BatchExecutor.getInstance().logInfo("Fatal error encountered!");
                System.exit(-1);
            }
        } catch (Throwable t) {
            instance.logInfo("Caught "+ t.getLocalizedMessage());
            instance.logError(t);
            System.exit(-1);
        } finally {
            cleanup(state, initTime);
        }

    }

    public static void init(String settingsFilePath, String name)
    throws InvalidPropertiesFormatException, IOException {
        if (instance == null) {
            instance = new BatchExecutor(settingsFilePath, name);
        }
    }

    public static BatchExecutor getInstance() {
        return instance;
    }

    public void registerFileForCleanup(File f) {
        filesToClean.add(f);
    }

    public void registerFileForCleanup(String f) {
        filesToClean.add(new File(f));
    }

    public synchronized void logDebug(String msg) {
        logger.mainLogger.debug(msg);
    }

    public synchronized void logInfo(String msg) {
        logger.mainLogger.info(msg);
    }
    
    public synchronized void logWarning(String msg) {
        logger.mainLogger.warn(msg);
    }

    public synchronized void logError(Throwable t) {
        String msg = t.getLocalizedMessage();
        logger.mainLogger.error(msg);
        logger.errorLogger.error(msg, t);
    }
    
    public synchronized void logError(String errorDesc) {
        logger.mainLogger.error(errorDesc);
        logger.errorLogger.error(errorDesc);
    }

    public File getLogFolder() {
        return logger.logFolder;
    }

    public String getProperty(Class<?> owner, String key) {
        return settings.getProperty(settings.getPropertyKey(owner, key));
    }

    public int getIntProperty(Class<?> owner, String key) {
        return Integer.parseInt(settings.getProperty(settings.getPropertyKey(owner, key)));
    }

    public int getIntProperty(Class<?> owner, String key, int defaultValue) {
        String value = settings.getProperty(settings.getPropertyKey(owner, key));
        return (value == null ? defaultValue : Integer.parseInt(value));
    }

    public long getLongProperty(Class<?> owner, String key) {
        return Long.parseLong(settings.getProperty(settings.getPropertyKey(owner, key)));
    }

    public long getLongProperty(Class<?> owner, String key, long defaultValue) {
        String value = settings.getProperty(settings.getPropertyKey(owner, key));
        return (value == null ? defaultValue : Long.parseLong(value));
    }

    public boolean getBoolProperty(Class<?> owner, String key) {
        return Boolean.parseBoolean(
                settings.getProperty(settings.getPropertyKey(owner, key)));
    }

    public boolean getBoolProperty(
            Class<?> owner, String key, boolean defaultValue) {
        String value = settings.getProperty(settings.getPropertyKey(owner, key));
        return (value == null ? defaultValue : Boolean.parseBoolean(value));
    }

    public static void cleanup(BatchState state, long initTime) {
        // Register error log for cleanup if it's empty
        File errorLog = new File(instance.logger.errorLogFileName);
        if (errorLog.exists() && (errorLog.length() == 0)) {
            instance.registerFileForCleanup(errorLog);
        }

        // Clean registered files
        for (File f : instance.filesToClean) {
        	try {
				FileUtils.recursiveDelete(f);
			} catch (final IOException e) {
				instance.logError(e);
			}
        }

        if (state != null) {
        	state.logStatus();
        }
        
        long timeElapsed = System.currentTimeMillis() - initTime;
        instance.logInfo("Time elapsed: " + TimeFormatter.formatDuration(timeElapsed));
        LogManager.shutdown();
        
        instance = null;
    }

}
