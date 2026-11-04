"""Shared spatial policy for the reference fixtures and future analytical queries."""

# ST_Project and spheroidal ST_DWithin can differ at the sub-micrometre boundary.
# This arithmetic tolerance is unrelated to positional accuracy or postcode fallback.
DISTANCE_EPSILON_METRES = 0.000001
