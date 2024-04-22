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
        // RLE Encoding logic
        for (int i = 0; i < bitmap.length; i++) {
            int runLength = 1;
            while (i + 1 < bitmap.length && bitmap[i] == bitmap[i + 1]) {
                runLength++;
                i++;
            }
            // Store run length and value; this is a simplified approach
            encoded.add((byte) runLength);
            encoded.add(bitmap[i]);
        }
        // Convert List<Byte> to byte[]
        byte[] result = new byte[encoded.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = encoded.get(i);
        }
        return result;
    }

    // Method to decode the RLE encoded bitmap
    public static byte[] decodeRLE(byte[] encodedBitmap) {
        List<Byte> decoded = new ArrayList<>();
        // RLE Decoding logic
        for (int i = 0; i < encodedBitmap.length; i += 2) {
            byte runLength = encodedBitmap[i];
            byte value = encodedBitmap[i + 1];
            for (int j = 0; j < runLength; j++) {
                decoded.add(value);
            }
        }
        // Convert List<Byte> to byte[]
        byte[] result = new byte[decoded.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = decoded.get(i);
        }
        return result;
    }
}