#!/usr/bin/env python3
from src.utils import parse_args
from src.simulation_runner import main


if __name__ == "__main__":
    args = parse_args()
    main(args)
