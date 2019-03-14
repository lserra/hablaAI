#!/usr/bin/python
# -*- coding: utf-8 -*-

# ==============================================================================
#             R U N N I N G    J O B S    A N D    T A S K S
# ==============================================================================
# Created by: Laercio Serra (laercio.serra@gmail.com)
# Customer: Habla AI (Geanderson Lenz via UpWork)
# ==============================================================================
# This script executes all jobs/tasks: extraction, transformation, and loading.
# ==============================================================================
import dt_extract as extraction
import dt_transform as transformation
import dt_load as loading


def main():
    """
    Jobs/tasks to be executed
    """
    extraction.run()
    transformation.run()
    loading.run()


if __name__ == '__main__':
    main()

