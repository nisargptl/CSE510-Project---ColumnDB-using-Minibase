package bitmap;// Additional imports
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

// New CBitMapPage class to handle RLE encoding/decoding
class CBitMapPage extends BMPage {
    // Assuming BMPage is a class you have that represents a bitmap page

    // Method to encode the bitmap as RLE
    public static byte[] encodeRLE(byte[] bitmap) {
        List<Byte> encoded = new ArrayList<>();
        for (int i = 0; i < bitmap.length; i++) {
            int runLength = 1;
            while (i + 1 < bitmap.length && bitmap[i] == bitmap[i + 1]) {
                runLength++;
                i++;
            }
            encoded.add((byte) runLength);
            encoded.add(bitmap[i]);
        }
        byte[] result = new byte[encoded.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = encoded.get(i);
        }
        return result;
    }

    // Decode the RLE encoded bitmap
    public static byte[] decodeRLE(byte[] encodedBitmap) {
        List<Byte> decoded = new ArrayList<>();
        for (int i = 0; i < encodedBitmap.length; i += 2) {
            byte runLength = encodedBitmap[i];
            byte value = encodedBitmap[i + 1];
            for (int j = 0; j < runLength; j++) {
                decoded.add(value);
            }
        }
        byte[] result = new byte[decoded.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = decoded.get(i);
        }
        return result;
    }
}
