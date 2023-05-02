from task1 import task1
from task2 import task2
from task3 import task3
from task4 import task4
from task5 import task5
from task6 import task6
from task7 import task7
from task8 import task8


def main():
    # Task 1
    path_to_save1 = "task_results\\Task1\\"
    path1 = r"imdb_data\title.akas.tsv.gz"
    task1(path1, path_to_save1)

    # Task 2
    path_to_save2 = "task_results\\Task2\\"
    path2 = r"imdb_data\name.basics.tsv.gz"
    task2(path2, path_to_save2)

    # Task 3
    path_to_save3 = "task_results\\Task3\\"
    path3 = r"imdb_data\title.basics.tsv.gz"
    task3(path3, path_to_save3)

    # Task 4
    path_to_save4 = "task_results\\Task4\\"
    path4 = r"imdb_data\title.principals.tsv.gz"
    path4add_name = r"imdb_data\name.basics.tsv.gz"
    path4add_title = r"imdb_data\title.basics.tsv.gz"
    task4(path4, path_to_save4, path4add_name, path4add_title)

    # Task 5
    path_to_save5 = "task_results\\Task5\\"
    path5 = r"imdb_data\title.basics.tsv.gz"
    path5add_region = r"imdb_data\title.akas.tsv.gz"
    task5(path5, path_to_save5, path5add_region)

    # Task 6
    path_to_save6 = "task_results\\Task6\\"
    path6 =  r"imdb_data\title.episode.tsv.gz"
    path6add_basics =  r"imdb_data\title.basics.tsv.gz"
    task6(path6, path_to_save6, path6add_basics)

    # Task 7
    path_to_save7 = "task_results\\Task7\\"
    path7 = r"imdb_data\title.ratings.tsv.gz"
    path7add_basics = r"imdb_data\title.basics.tsv.gz"
    task7(path7, path_to_save7, path7add_basics)

    # Task 8
    path_to_save8 = "task_results\\Task8\\"
    path_rating = r"imdb_data\title.ratings.tsv.gz"
    path_basics = r"imdb_data\title.basics.tsv.gz"
    task8(path_rating, path_to_save8, path_basics)


if __name__ == "__main__":
    main()
