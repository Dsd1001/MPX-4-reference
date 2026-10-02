import unittest

from mpx4.codec import (
    ResetStream,
    StopSending,
    StreamCredit,
    StreamData,
    StreamOpenReject,
    StreamTerminal,
    TransmissionAck,
    decode_frames,
    decode_reset_stream,
    decode_transmission_ack,
)
from mpx4.constants import ErrorCode, FrameType
from mpx4.errors import StreamStateError, TransmissionIDError
from mpx4.opening import ClientOpeningState
from mpx4.stream import SendFlow, TransmissionLedger


def one_frame(wire: bytes):
    frames = decode_frames(wire)
    if len(frames) != 1:
        raise AssertionError("expected exactly one Frame")
    return frames[0]


def new_opening():
    ledger = TransmissionLedger()
    open_txid = ledger.allocate(1, "open")
    send_flow = SendFlow()
    state = ClientOpeningState(
        stream_id=1,
        open_transmission_id=open_txid,
        ledger=ledger,
        send_flow=send_flow,
    )
    return state, ledger, send_flow, open_txid


class ClientOpeningAcceptanceEvidenceTests(unittest.TestCase):
    def test_credit_before_open_ok_is_acceptance_evidence(self):
        state, ledger, send_flow, open_txid = new_opening()

        state.observe_credit(StreamCredit(1, 0, 65536))
        self.assertTrue(state.acceptance_evidence)
        self.assertEqual(
            state.phase,
            "OPENING_WITH_ACCEPTANCE_EVIDENCE",
        )
        self.assertEqual(send_flow.stream_credit[1].maximum, 65536)

        result = state.observe_open_ok(1, open_txid)
        self.assertTrue(result.became_open)
        self.assertEqual(state.phase, "OPEN")
        self.assertIn(open_txid, ledger.settled)

    def test_fin_zero_before_open_ok_is_acceptance_evidence_and_is_acked(self):
        state, _, _, open_txid = new_opening()

        result = state.observe_fin(StreamTerminal(1, 50, 0))
        self.assertEqual(state.phase, "OPENING_WITH_ACCEPTANCE_EVIDENCE")
        self.assertEqual(state.receive.terminal_kind, "fin")
        self.assertTrue(state.receive.complete)

        ack = decode_transmission_ack(one_frame(result.responses[0]))
        self.assertEqual((ack.stream_id, ack.transmission_id), (1, 50))
        state.observe_open_ok(1, open_txid)
        self.assertEqual(state.phase, "OPEN")

    def test_reset_zero_before_open_ok_is_acceptance_evidence_and_authoritative(self):
        state, _, _, open_txid = new_opening()

        result = state.observe_reset(ResetStream(1, 51, 0, 77))
        self.assertEqual(state.receive.terminal_kind, "reset")
        self.assertEqual(state.peer_terminal_error_code, 77)
        ack = decode_transmission_ack(one_frame(result.responses[0]))
        self.assertEqual(ack.transmission_id, 51)

        state.observe_fin(StreamTerminal(1, 52, 0))
        self.assertEqual(state.receive.terminal_kind, "reset")
        state.observe_open_ok(1, open_txid)

    def test_stop_before_open_ok_acks_and_creates_reset_zero(self):
        state, ledger, _, open_txid = new_opening()

        result = state.observe_stop(StopSending(1, 60, 88))
        self.assertEqual(len(result.responses), 2)
        ack = decode_transmission_ack(one_frame(result.responses[0]))
        reset = decode_reset_stream(one_frame(result.responses[1]))
        self.assertEqual((ack.stream_id, ack.transmission_id), (1, 60))
        self.assertEqual(reset.stream_id, 1)
        self.assertEqual(reset.final_offset, 0)
        self.assertEqual(reset.error_code, 88)
        self.assertEqual(
            reset.transmission_id,
            state.local_reset_transmission_id,
        )

        duplicate = state.observe_stop(StopSending(1, 60, 88))
        duplicate_reset = decode_reset_stream(one_frame(duplicate.responses[1]))
        self.assertEqual(
            duplicate_reset.transmission_id,
            reset.transmission_id,
        )

        state.observe_ack(
            TransmissionAck(1, reset.transmission_id, 0)
        )
        self.assertTrue(state.local_reset_acked)
        self.assertIn(reset.transmission_id, ledger.settled)

        after_ack = state.observe_stop(StopSending(1, 60, 88))
        self.assertEqual(len(after_ack.responses), 1)
        state.observe_open_ok(1, open_txid)
        self.assertEqual(state.phase, "OPEN")

    def test_data_before_open_ok_is_stream_state_error(self):
        state, _, _, _ = new_opening()
        with self.assertRaises(StreamStateError):
            state.observe_data(StreamData(1, 0, 70, b"x"))

    def test_acceptance_evidence_makes_later_reject_invalid(self):
        state, _, _, open_txid = new_opening()
        state.observe_credit(StreamCredit(1, 0, 4096))

        with self.assertRaises(StreamStateError):
            state.observe_open_reject(
                StreamOpenReject(
                    1,
                    open_txid,
                    int(ErrorCode.STREAM_LIMIT),
                )
            )

    def test_reject_without_evidence_is_terminal_and_duplicate_is_idempotent(self):
        state, ledger, _, open_txid = new_opening()
        reject = StreamOpenReject(
            1,
            open_txid,
            int(ErrorCode.STREAM_LIMIT),
        )

        result = state.observe_open_reject(reject)
        self.assertTrue(result.became_rejected)
        self.assertEqual(state.phase, "REJECTED")
        self.assertIn(open_txid, ledger.settled)

        duplicate = state.observe_open_reject(reject)
        self.assertFalse(duplicate.became_rejected)

        with self.assertRaises(StreamStateError):
            state.observe_open_reject(
                StreamOpenReject(
                    1,
                    open_txid,
                    int(ErrorCode.RESOURCE_LIMIT),
                )
            )

    def test_nonzero_terminal_evidence_is_invalid_while_opening(self):
        state, _, _, _ = new_opening()
        with self.assertRaises(StreamStateError):
            state.observe_fin(StreamTerminal(1, 80, 1))

        state, _, _, _ = new_opening()
        with self.assertRaises(StreamStateError):
            state.observe_reset(ResetStream(1, 81, 1, 7))

    def test_open_ok_wrong_transmission_is_rejected(self):
        state, _, _, _ = new_opening()
        with self.assertRaises(TransmissionIDError):
            state.observe_open_ok(1, 999)


if __name__ == "__main__":
    unittest.main()
