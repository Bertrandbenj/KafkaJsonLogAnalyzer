package org.nimajneb.kafka;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.util.Random;


public class KLoggerGenerator {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final Random r = new Random();

    public static final Marker FLOW = MarkerManager.getMarker("FLOW");

    public static final Marker SUPERVISION = MarkerManager.getMarker("SUPERVISION");

    public static final Marker ALERT = MarkerManager.getMarker("ALERT").addParents(FLOW);

    public static final Marker PROJECT = MarkerManager.getMarker("PROJECT").addParents(SUPERVISION,ALERT);

    public static void main(String... args){

        Runnable job1 = () -> supervise("project1.joinAandB.count");
        Runnable job2 = () -> supervise("project1.A.count" );
        Runnable job3 = () -> supervise("project1.B.count" );
        Runnable job4 = () -> supervise("project2.cols.count" );
        Runnable job5 = () -> supervise("project2.table1.schema"  );
        Runnable job6 = () -> supervise("project2.cols.count");

        Runnable buggytask = () -> alert("project1.function1.dataMissing tableX");
        Runnable buggytask1 = () -> alert("project2.function1.exception");
        Runnable buggytask2 = () -> alert("project2.function2.exception");

        new Thread(job1).start();
        new Thread(job2).start();
        new Thread(job3).start();
        new Thread(job4).start();
        new Thread(job5).start();
        new Thread(job6).start();

        new Thread(buggytask).start();
        new Thread(buggytask1).start();
        new Thread(buggytask2).start();
    }

    private static void alert(String message) {
        while (true) {
            LOGGER.error(ALERT, message + " " + (1000+r.nextInt(1000)), new Exception("HUHU"));
            try {
                Thread.sleep(5000 + r.nextInt(1000));
            } catch (InterruptedException e) {
                LOGGER.error("ERRORRRRRR", e);
            }
        }
    }

    private static void supervise( String message ) {
        while (true) {
            LOGGER.info(PROJECT, message + " " + (1000+r.nextInt(1000)));
            try {
                Thread.sleep(2000 + r.nextInt(1000));
            } catch (InterruptedException e) {
                LOGGER.error("ERRORRRRRR", e);
            }
        }
    }
}
