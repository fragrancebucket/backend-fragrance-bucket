"""Pure, deterministic auction rules. Persistence and broadcasts live outside this module."""
from dataclasses import dataclass
from random import SystemRandom

PRESETS = (20, 30, 50, 100)
MAX_ROUNDS = 5


@dataclass(frozen=True)
class RoundResolution:
    outcome: str  # unsold, sold, next_round
    price: int | None = None
    eligible_user_ids: tuple[str, ...] = ()
    winner_id: str | None = None
    random_reason: str | None = None


def validate_bid(*, amount: int, floor: int, previous_amount: int | None) -> None:
    if amount < floor:
        raise ValueError("Bid is below current minimum.")
    if previous_amount is not None and amount < previous_amount:
        raise ValueError("Bid cannot reduce your current-round bid.")


def resolve_round(*, round_number: int, bids: dict[str, int], prior_tied_users: tuple[str, ...] = (), prior_price: int | None = None, chooser=None) -> RoundResolution:
    if not bids:
        if round_number == 1:
            return RoundResolution(outcome="unsold")
        winner = (chooser or SystemRandom().choice)(prior_tied_users)
        return RoundResolution("sold", prior_price, prior_tied_users, winner, "no_bids_in_tie_round")

    high = max(bids.values())
    tied = tuple(sorted(user_id for user_id, amount in bids.items() if amount == high))
    if len(tied) == 1:
        return RoundResolution("sold", high, tied, tied[0])
    if round_number == MAX_ROUNDS:
        winner = (chooser or SystemRandom().choice)(tied)
        return RoundResolution("sold", high, tied, winner, "round_5_tie")
    return RoundResolution("next_round", high, tied)
