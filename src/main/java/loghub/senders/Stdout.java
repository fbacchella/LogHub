package loghub.senders;

import java.io.IOException;
import java.io.PrintStream;

import loghub.Event;
import loghub.Sender;
import loghub.configuration.Beans;

@Beans("destination")
public class Stdout extends Sender {

    PrintStream destination = System.out;
    
    @Override
    public void send(Event event) {
        try {
            byte[] msg = getEncoder().encode(event);
            System.out.write(msg);
            System.out.println();
            System.out.flush();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }

    @Override
    public String getSenderName() {
        return "stdout";
    }

    public String getDestination() {
        return destination == System.out ? "stdout": "stderr";
    }

    public void setDestination(String destination) {
        switch(destination){
        case "stdout": this.destination = System.out; break;
        case "stderr": this.destination = System.out; break;
        }
    }

}
