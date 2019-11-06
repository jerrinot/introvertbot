package info.jerrinot.introvertbot.source;

import info.jerrinot.introvertbot.DetectedObject;
import info.jerrinot.introvertbot.Frame;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class JsonParserTest {

    @Test
    public void testSingleObject() {
        JsonParser parser = new JsonParser();

        assertNull(parser.feed("["));
        assertNull(parser.feed("{"));
        assertNull(parser.feed(" \"frame_id\":531, "));
        assertNull(parser.feed(" \"objects\": [ "));
        assertNull(parser.feed("  {\"class_id\":0, \"name\":\"person\", \"relative_coordinates\":{\"center_x\":0.652169, \"center_y\":0.686165, \"width\":0.698665, \"height\":0.614011}, \"confidence\":0.999583}"));
        assertNull(parser.feed(" ] "));
        Frame frame = parser.feed("}, ");
        assertNotNull(frame);
        assertEquals(531, frame.getId());
        List<DetectedObject> detectedObjs = frame.getObjects();
        assertEquals(1, detectedObjs.size());
        DetectedObject detectedObj = detectedObjs.get(0);
        assertEquals("person", detectedObj.getName());
        assertEquals(0.999583, detectedObj.getConfidence(), 0.00000001);
    }

    @Test
    public void testNoObjectDetected() {
        JsonParser parser = new JsonParser();

        assertNull(parser.feed("["));
        assertNull(parser.feed("{"));
        assertNull(parser.feed(" \"frame_id\":531, "));
        assertNull(parser.feed(" \"objects\": [ "));
        assertNull(parser.feed(" "));
        assertNull(parser.feed(" ] "));
        Frame frame = parser.feed("}, ");

    }
}