package loghub.events;

import static org.mockito.Mockito.mock;

public class Mocker {

    private Mocker() {
        //Empty
    }

    public static Event getMock() {
        return mock(EventInstance.class);
    }

}
