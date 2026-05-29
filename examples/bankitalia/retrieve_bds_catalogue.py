"""Discover Bank of Italy Statistical Database cubes."""

from italian_our_world_data import list_bankitalia_bds_cubes


def main() -> None:
    cubes = list_bankitalia_bds_cubes(max_depth=3, limit=10)
    print(cubes[["cube_id", "local_id", "name", "last_update"]].head())


if __name__ == "__main__":
    main()
