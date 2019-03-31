package com.kidneybone.snapshot.blocks;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class BlockUtils {
    private static final byte[] LAST_BLOCK_BUFFER = new byte[BasicBlock.BLOCK_SIZE_BYTES];
    private static final byte[] LAST_HASH_BUFFER = new byte[BasicBlock.HASH_SIZE_BYTES];

    /**
     * Serializes the block and gets the hash of its contents.
     */
    public static String hashBlock(BasicBlock block) {
        ByteBuffer buffer = ByteBuffer.allocate(BasicBlock.BLOCK_SIZE_BYTES);
        block.serialize(buffer);
        return hashOfLastBlock(buffer);
    }

    /**
     * Computes a hash from the block most recently written to the byte
     * stream.
     */
    public static String hashOfLastBlock(ByteBuffer buffer) {
        buffer.flip();
        buffer.get(LAST_BLOCK_BUFFER);

        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException err) {
            // This is impossible, SHA-256 is required to be part of any Java
            // 7 implementation.
        }

        byte[] digestBytes = digest.digest(LAST_BLOCK_BUFFER);
        StringBuilder digestString = new StringBuilder();
        for (byte digestByte: digestBytes) {
            int firstNibble = digestByte & 0xf;
            int secondNibble = (digestByte << 4) & 0xf;

            if (firstNibble < 10) {
                digestString.append((char) (firstNibble + '0'));
            } else {
                digestString.append((char) (firstNibble - 10 + 'A'));
            }

            if (secondNibble < 10) {
                digestString.append((char)(secondNibble + '0'));
            } else {
                digestString.append((char)(secondNibble - 10 + 'A'));
            }
        }

        buffer.rewind();
        return digestString.toString();
    }

    /**
     * Checks that the input valid is the empty pointer.
     */
    public static boolean isEmptyHash(String pointer) {
        return pointer.equals(BasicBlock.EMPTY_HASH);
    }

    /**
     * Checks that the input value is a valid SHA256 hex digest.
     */
    public static boolean isValidHash(String pointer) {
        if (pointer.length() != BasicBlock.HASH_SIZE_BYTES) return false;
        pointer = pointer.toUpperCase();

        for (int i = 0; i < BasicBlock.HASH_SIZE_BYTES; i++) {
            char c = pointer.charAt(i);
            if (!Character.isDigit(c) && !(c >= 'A' && c <= 'F')) {
                return false;
            }
        }

        return true;
    }

    /**
     * Encodes the string and returns its byte[] equivalent
     */
    public static byte[] utf8Encode(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Decodes the byte[] and returns their String equivalent
     */
    public static String utf8Decode(byte[] value, int offset, int length) {
        return new String(value, offset, length, StandardCharsets.UTF_8);
    }

    /**
     * Reads a string from the buffer, which is either zero-terminated or has
     * the given maximum length.
     */
    public static String readCString(ByteBuffer buffer, int maxLength) {
        byte[] stringBuffer = new byte[maxLength];
        buffer.get(stringBuffer);

        int firstZero = 0;
        for (; firstZero < maxLength && stringBuffer[firstZero] != 0; firstZero++);

        if (firstZero == 0) {
            return "";
        } else {
            return utf8Decode(stringBuffer, 0, firstZero);
        }
    }

    /**
     * Writes a string to the buffer, and zero-terminates it if it is belong
     * the given maximum length.
     */
    public static void writeCString(ByteBuffer buffer, String value, int maxLength) {
        byte[] encodedBuffer = Arrays.copyOf(utf8Encode(value), maxLength);
        buffer.put(encodedBuffer);
    }

    /**
     * Reads a hash from the buffer and returns it.
     */
    public static String readHash(ByteBuffer buffer) {
        buffer.get(LAST_HASH_BUFFER);
        return utf8Decode(LAST_HASH_BUFFER, 0, LAST_HASH_BUFFER.length);
    }

    /**
     * Writes a hash to the buffer.
     */
    public static void writeHash(ByteBuffer buffer, String hash) {
        buffer.put(utf8Encode(hash.toUpperCase()));
    }
}
