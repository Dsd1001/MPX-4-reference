import unittest

from mpx4.codec import (
    decode_frame,
    decode_frames,
    encode_ping,
    encode_stream_data,
)
from mpx4.constants import FrameType
from mpx4.errors import DecodeError, NeedMoreData
from mpx4.varint import decode_varint_exact, encode_varint


class VarIntTests(unittest.TestCase):
    def test_boundaries(self):
        values = [0, 1, 63, 64, 16383, 16384, (1 << 30) - 1, 1 << 30, (1 << 62) - 1]
        widths = [1, 1, 1, 2, 2, 4, 4, 8, 8]
        for value, width in zip(values, widths):
            wire = encode_varint(value)
            self.assertEqual(len(wire), width)
            self.assertEqual(decode_varint_exact(wire), value)

    def test_noncanonical_rejected(self):
        with self.assertRaises(DecodeError):
            decode_varint_exact(bytes.fromhex("4000"))

    def test_truncated_rejected(self):
        with self.assertRaises(NeedMoreData):
            decode_varint_exact(bytes.fromhex("40"))


class FrameTests(unittest.TestCase):
    def test_stream_data_known_encoding(self):
        wire = encode_stream_data(1, 32768, 7, b"hello")
        self.assertEqual(wire.hex(), "130b01800080000768656c6c6f")
        frame, end = decode_frame(wire)
        self.assertEqual(end, len(wire))
        self.assertEqual(frame.type, int(FrameType.STREAM_DATA))

    def test_multiple_frames(self):
        plaintext = encode_ping(1) + encode_ping(2)
        frames = decode_frames(plaintext)
        self.assertEqual([f.type for f in frames], [1, 1])


if __name__ == "__main__":
    unittest.main()
