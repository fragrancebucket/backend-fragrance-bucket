import pytest
from app.auction_engine import resolve_round, validate_bid


def test_first_round_without_bids_is_unsold():
    assert resolve_round(round_number=1, bids={}).outcome == "unsold"


def test_unique_high_bid_wins():
    result = resolve_round(round_number=1, bids={"a": 2000, "b": 2050})
    assert (result.outcome, result.winner_id, result.price) == ("sold", "b", 2050)


def test_tied_users_only_continue():
    result = resolve_round(round_number=1, bids={"a": 2050, "b": 2100, "c": 2100})
    assert result.outcome == "next_round"
    assert result.eligible_user_ids == ("b", "c")


def test_silent_tie_round_randomizes_prior_tie():
    result = resolve_round(round_number=2, bids={}, prior_tied_users=("a", "b"), prior_price=2300, chooser=lambda _: "b")
    assert (result.winner_id, result.price, result.random_reason) == ("b", 2300, "no_bids_in_tie_round")


def test_round_five_tie_randomizes_current_tie():
    result = resolve_round(round_number=5, bids={"a": 2400, "b": 2400}, chooser=lambda _: "a")
    assert (result.winner_id, result.random_reason) == ("a", "round_5_tie")


def test_bid_cannot_be_below_floor_or_prior_bid():
    with pytest.raises(ValueError): validate_bid(amount=1999, floor=2000, previous_amount=None)
    with pytest.raises(ValueError): validate_bid(amount=2050, floor=2000, previous_amount=2100)
