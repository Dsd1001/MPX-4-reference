import unittest

from mpx4.codec import (
    ResetStream,
    StopSending,
    StreamData,
    StreamTerminal,
    TransmissionAck,
    decode_frames,
    decode_reset_stream,
    decode_stream_fin,
    decode_stream_open_reject,
    decode_stop_sending,
)
from mpx4.constants import ErrorCode, FrameType
from mpx4.errors import FinalSizeError, StreamStateError, TransmissionIDError
from mpx4.terminal import ServerTerminalStateMachine


def one_frame(wire: bytes):
    frames = decode_frames(wire)
    if len(frames) != 1:
        raise AssertionError("expected exactly one Frame")
    return frames[0]


class TerminalCodecTests(unittest.TestCase):
    def test_reset_stop_and_reject_typed_decoders(self):
        from mpx4.codec import (
            encode_reset_stream,
            encode_stop_sending,
            encode_stream_open_reject,
        )

        reset = decode_reset_stream(
            one_frame(encode_reset_stream(1, 9, 17, 23))
        )
        self.assertEqual(reset, ResetStream(1, 9, 17, 23))

        stop = decode_stop_sending(
            one_frame(encode_stop_sending(3, 10, 24))
        )
        self.assertEqual(stop, StopSending(3, 10, 24))

        reject = decode_stream_open_reject(
            one_frame(
                encode_stream_open_reject(
                    5,
                    11,
                    int(ErrorCode.STREAM_STATE_ERROR),
                )
            )
        )
        self.assertEqual(reject.stream_id, 5)
        self.assertEqual(reject.transmission_id, 11)
        self.assertEqual(
            reject.error_code,
            int(ErrorCode.STREAM_STATE_ERROR),
        )


class PreOpenCancellationTests(unittest.TestCase):
    def test_preopen_reset_zero_tombstones_and_rejects_late_open(self):
        machine = ServerTerminalStateMachine(max_streams=1)

        result = machine.handle_reset(
            ResetStream(
                stream_id=1,
                transmission_id=10,
                final_offset=0,
                error_code=77,
            )
        )
        self.assertEqual(machine.active_stream_count, 0)
        self.assertIn(1, machine.tombstones)
        self.assertFalse(result.application_created)
        self.assertEqual(
            one_frame(result.responses[0]).type,
            int(FrameType.TRANSMISSION_ACK),
        )

        late_open = machine.handle_open(1, 11)
        self.assertFalse(late_open.application_created)
        reject = decode_stream_open_reject(one_frame(late_open.responses[0]))
        self.assertEqual(reject.stream_id, 1)
        self.assertEqual(reject.transmission_id, 11)
        self.assertEqual(
            reject.error_code,
            int(ErrorCode.STREAM_STATE_ERROR),
        )
        self.assertNotIn(1, machine.active)

        # Tombstones do not consume MAX_STREAMS active capacity.
        accepted = machine.handle_open(3, 12)
        self.assertTrue(accepted.application_created)
        self.assertEqual(machine.active_stream_count, 1)

    def test_preopen_reset_nonzero_is_stream_state_error(self):
        machine = ServerTerminalStateMachine()
        with self.assertRaises(StreamStateError):
            machine.handle_reset(
                ResetStream(1, 10, 1, 77)
            )
        self.assertNotIn(1, machine.tombstones)
        self.assertNotIn(1, machine.registry.used)

    def test_preopen_stop_acks_sends_reset_and_rejects_late_open(self):
        machine = ServerTerminalStateMachine()

        result = machine.handle_stop(
            StopSending(
                stream_id=1,
                transmission_id=20,
                error_code=88,
            )
        )
        self.assertEqual(len(result.responses), 2)
        ack, reset_wire = result.responses
        self.assertEqual(
            one_frame(ack).type,
            int(FrameType.TRANSMISSION_ACK),
        )
        reset = decode_reset_stream(one_frame(reset_wire))
        self.assertEqual(reset.stream_id, 1)
        self.assertEqual(reset.final_offset, 0)
        self.assertEqual(reset.error_code, 88)

        duplicate = machine.handle_stop(
            StopSending(1, 20, 88)
        )
        duplicate_reset = decode_reset_stream(
            one_frame(duplicate.responses[1])
        )
        self.assertEqual(
            duplicate_reset.transmission_id,
            reset.transmission_id,
        )

        late_open = machine.handle_open(1, 21)
        reject = decode_stream_open_reject(one_frame(late_open.responses[0]))
        self.assertEqual(
            reject.error_code,
            int(ErrorCode.STREAM_STATE_ERROR),
        )

    def test_preopen_stop_duplicate_cannot_change_semantics(self):
        machine = ServerTerminalStateMachine()
        machine.handle_stop(StopSending(1, 20, 88))
        with self.assertRaises(TransmissionIDError):
            machine.handle_stop(StopSending(1, 21, 88))
        with self.assertRaises(StreamStateError):
            machine.handle_stop(StopSending(1, 20, 89))


class FinalSizeAndResetTests(unittest.TestCase):
    def _open(self):
        machine = ServerTerminalStateMachine()
        machine.handle_open(1, 1)
        return machine

    def test_fin_then_reset_same_final_makes_reset_authoritative(self):
        machine = self._open()
        machine.handle_data(StreamData(1, 0, 10, b"abc"))
        machine.handle_fin(StreamTerminal(1, 11, 3))

        result = machine.handle_reset(
            ResetStream(1, 12, 3, 99)
        )
        self.assertEqual(len(result.responses), 1)
        state = machine.active[1]
        self.assertEqual(state.receive.final_offset, 3)
        self.assertEqual(state.receive.terminal_kind, "reset")
        self.assertEqual(state.peer_terminal_transmission_id, 12)
        self.assertEqual(state.peer_error_code, 99)

        # A later FIN with the same final size is acknowledged but cannot
        # replace reset semantics.
        machine.handle_fin(StreamTerminal(1, 11, 3))
        self.assertEqual(state.receive.terminal_kind, "reset")

    def test_terminal_final_size_cannot_change(self):
        machine = self._open()
        machine.handle_fin(StreamTerminal(1, 11, 0))
        with self.assertRaises(FinalSizeError):
            machine.handle_reset(ResetStream(1, 12, 1, 99))

    def test_terminal_retransmission_must_keep_transmission_identity(self):
        machine = self._open()
        machine.handle_fin(StreamTerminal(1, 11, 0))
        with self.assertRaises(TransmissionIDError):
            machine.handle_fin(StreamTerminal(1, 12, 0))

        machine = self._open()
        machine.handle_reset(ResetStream(1, 13, 0, 44))
        with self.assertRaises(TransmissionIDError):
            machine.handle_reset(ResetStream(1, 14, 0, 44))
        with self.assertRaises(StreamStateError):
            machine.handle_reset(ResetStream(1, 13, 0, 45))

    def test_late_data_after_reset_is_not_delivered_and_beyond_final_fails(self):
        machine = self._open()
        machine.handle_data(StreamData(1, 0, 10, b"ab"))
        machine.handle_reset(ResetStream(1, 11, 4, 55))

        late = machine.handle_data(StreamData(1, 2, 12, b"cd"))
        self.assertEqual(late.delivered, b"")
        self.assertEqual(
            machine.receive_flow.stream_committed[1],
            4,
        )
        with self.assertRaises(FinalSizeError):
            machine.handle_data(StreamData(1, 4, 13, b"x"))


class StopSendingTests(unittest.TestCase):
    def test_stop_sending_supersedes_pending_fin_without_stale_ack_cancelling_reset(self):
        machine = ServerTerminalStateMachine()
        machine.handle_open(1, 1)
        machine.note_local_send_commit(1, 5)

        fin_wire = machine.start_local_fin(1)
        fin = decode_stream_fin(one_frame(fin_wire))
        stop_result = machine.handle_stop(
            StopSending(1, 50, 123)
        )
        self.assertEqual(len(stop_result.responses), 2)
        reset = decode_reset_stream(one_frame(stop_result.responses[1]))

        self.assertEqual(reset.final_offset, 5)
        self.assertEqual(reset.error_code, 123)
        self.assertNotEqual(reset.transmission_id, fin.transmission_id)
        self.assertEqual(machine.active[1].local_terminal_kind, "reset")

        # The superseded FIN ACK settles only the old FIN Transmission.
        machine.handle_ack(
            TransmissionAck(1, fin.transmission_id, 0)
        )
        self.assertFalse(machine.active[1].local_terminal_acked)

        machine.handle_ack(
            TransmissionAck(1, reset.transmission_id, 0)
        )
        self.assertTrue(machine.active[1].local_terminal_acked)


class TombstoneAndRetiredIdentityTests(unittest.TestCase):
    def _fully_close_to_tombstone(self):
        machine = ServerTerminalStateMachine()
        machine.handle_open(1, 100)

        local_fin_wire = machine.start_local_fin(1)
        local_fin = decode_stream_fin(one_frame(local_fin_wire))

        machine.handle_fin(StreamTerminal(1, 200, 0))
        consumed_wire = machine.release_receive(1)
        self.assertIsNotNone(consumed_wire)
        from mpx4.codec import decode_stream_consumed

        consumed = decode_stream_consumed(one_frame(consumed_wire))

        machine.handle_ack(
            TransmissionAck(1, local_fin.transmission_id, 0)
        )
        machine.handle_ack(
            TransmissionAck(1, consumed.transmission_id, 0)
        )
        self.assertNotIn(1, machine.active)
        self.assertIn(1, machine.tombstones)
        return machine

    def test_matching_terminal_duplicate_is_idempotent_and_conflict_fails(self):
        machine = self._fully_close_to_tombstone()

        duplicate = machine.handle_fin(StreamTerminal(1, 200, 0))
        self.assertEqual(len(duplicate.responses), 1)
        self.assertEqual(
            one_frame(duplicate.responses[0]).type,
            int(FrameType.TRANSMISSION_ACK),
        )

        with self.assertRaises(FinalSizeError):
            machine.handle_fin(StreamTerminal(1, 200, 1))

    def test_compacted_retired_identity_ignores_stale_frames_and_never_reopens(self):
        machine = self._fully_close_to_tombstone()
        before_session_commit = machine.receive_flow.session_committed

        machine.compact(1)
        self.assertIn(1, machine.retired)
        self.assertNotIn(1, machine.tombstones)

        stale_data = machine.handle_data(
            StreamData(1, 0, 300, b"stale")
        )
        self.assertEqual(stale_data.responses, ())
        self.assertEqual(stale_data.delivered, b"")
        self.assertEqual(
            machine.receive_flow.session_committed,
            before_session_commit,
        )

        reopen = machine.handle_open(1, 301)
        self.assertFalse(reopen.application_created)
        self.assertEqual(reopen.responses, ())
        self.assertNotIn(1, machine.active)

    def test_tombstones_and_retired_ids_do_not_count_toward_max_streams(self):
        machine = ServerTerminalStateMachine(max_streams=1)
        machine.handle_reset(ResetStream(1, 10, 0, 1))
        self.assertEqual(machine.active_stream_count, 0)
        machine.compact(1)

        accepted = machine.handle_open(3, 11)
        self.assertTrue(accepted.application_created)
        self.assertEqual(machine.active_stream_count, 1)


if __name__ == "__main__":
    unittest.main()
