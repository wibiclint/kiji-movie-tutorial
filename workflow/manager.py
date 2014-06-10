""" Python script to manage running jobs, checking the results, etc. """

# TODO: Should have an option to do a sanity check "kiji scan" after all operations

import argparse
import os
import pprint
import subprocess
import sys
from argparse import RawTextHelpFormatter


def run(cmd):
    print(cmd)
    return subprocess.check_output(cmd, shell=True).decode('utf-8')


class TimeInterval:
    def __init__(self, start, end):
        """ TODO: Check times formatting properly here """
        self.start = start
        self.end = end


class MovieAdvisorManager:
    # Put these into a sensible order
    possible_actions = [
        'help-actions',
        'install-bento',
        'create-tables',
        'import-ratings',
        'import-user-info',
        'import-movie-info',
        'train-item-item-cf',
    ]

    actions_help = {
        "install-bento":
            "Will set up a bento box for you.  Assumes that you are in a directory with a tar.gz "
            "file for the latest bento build.  This command will rm -rf your current bento box "
            "(which is also assumed to be in your current directory).",

        "create-tables":
            "Deletes the currently existing user and content tables and recreates them from DDL.",

        "import-ratings":
            "Run an express job to import movie ratings",

        "import-user-info":
            "Run an express job to import user information",

        "import-movie-info":
            "Run an express job to import movie information",

        "train-item-item-cf":
            "Calculate item-item similarities",

        "help-actions":
            "Print this help",
    }

    jars = (
        # 'train/target/train-1.0-SNAPSHOT-jar-with-dependencies.jar',
        #'schema/target/schema-1.0-SNAPSHOT-jar-with-dependencies.jar',
        'avro/target/movie-advisor-avro-1.0-SNAPSHOT.jar',
    )

    # This is the path from movie_advisor_home
    ddls = (
        'layout/src/main/resources/users.ddl',
        'layout/src/main/resources/content.ddl',
    )

    express_jar = 'express/target/express-1.0-SNAPSHOT.jar'

    # assert set(actions_help.keys()) == set(possible_actions)

    def _help_actions(self):
        """ Print detailed information about how the different actions work """
        actions_str = ""
        for (key, value) in self.actions_help.items():
            actions_str += "command: %s\n%s\n\n" % (key, value)
        print(actions_str)
        sys.exit(0)

    def _setup_parser(self):
        """ Add actions for the command-line arguments parser """
        parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter,
                                         description="Manage Kiji stuff for MovieAdvisor.  Available actions:\n\t" + \
                                                     "\n\t".join(self.possible_actions))

        # TODO: Detailed help information that prints out all of the available actions and their
        # assumptions

        parser.add_argument(
            "action",
            nargs='*',
            help="Action to take")

        parser.add_argument(
            '--bento-home',
            help='Location of bento box',
            default='kiji-bento-ebi')

        parser.add_argument(
            '--bento-tgz',
            help='Bento TAR file name',
            default='kiji-bento-ebi-2.0.2-release.tar.gz')

        parser.add_argument(
            '--movie-advisor-home',
            help='Location of checkout of WibiData MovieAdvisor github repo',
            default='movie-advisor')

        # Set up dates for training, testing, etc.
        parser.add_argument(
            '--train-start-date',
            default='2013-11-01')

        parser.add_argument(
            '--train-end-date',
            default='2013-11-15')

        parser.add_argument(
            '--test-start-date',
            default='2013-11-16')

        parser.add_argument(
            '--test-end-date',
            default='2013-11-30')

        parser.add_argument(
            "--backtest-results-file",
            default="backtest.txt")

        parser.add_argument(
            "--kill-bento",
            action="store_true",
            default=False,
            help="Automatically kill existing BentoBox processes.")

        parser.add_argument(
            "--show-classpath",
            action="store_true",
            default=False,
            help="Echo $KIJICLASSPATH and exit")

        return parser

    def _setup_environment_vars(self, opts):
        """ Set up useful variables (would be environment vars outside of the script) """
        # Check that these directories actually exist
        assert os.path.isdir(opts.movie_advisor_home)

        #if not 'install-bento' in self.actions: assert os.path.isdir(opts.bento_home)

        self.movie_advisor_home = opts.movie_advisor_home
        self.bento_home = opts.bento_home
        self.bento_tgz = opts.bento_tgz
        self.kiji_uri = "kiji://.env/dtv"

        # "express job" takes a jar file as an argument
        assert os.path.isfile(os.path.join(self.movie_advisor_home, self.express_jar))

        # Set the classpath for all of the commands that we'll run
        jarsFullPaths = [os.path.join(self.movie_advisor_home, j) for j in self.jars]
        for jar in jarsFullPaths: assert os.path.isfile(jar)

        classpath = ":".join(jarsFullPaths)
        os.environ['KIJI_CLASSPATH'] = classpath

        if opts.show_classpath:
            print("export KIJI_CLASSPATH=%s" % classpath)
            sys.exit(0)


    def _parse_options(self, args):
        """ Parse the command-line options and configure the script appropriately """
        parser = self._setup_parser()
        opts = parser.parse_args(args)

        self.actions = opts.action
        for action in self.actions:
            assert action in self.possible_actions, "Action %s is not a known action for the script" % action

        self.b_kill_bento = opts.kill_bento

        if 'help-actions' in self.actions: self._help_actions()

        self._setup_environment_vars(opts)
        self.backtest_results_file = opts.backtest_results_file

    def _exit_if_bento_still_running(self):
        jps_results = run('jps')
        if jps_results.lower().find('minicluster') != -1 and not self.b_kill_bento:
            assert False, "Please kill all bento-related jobs (run 'jps' to get a list)"

        # Kill all of the bento processes
        for line in jps_results.splitlines():
            toks = line.split()
            if len(toks) == 1: continue
            assert len(toks) == 2, toks
            (pid, job) = toks
            if job == 'Jps': continue
            cmd = "kill -9 " + pid
            run(cmd)

    def _do_action_bento_setup(self):
        """ Install the BentoBox, install Kiji, etc. """
        self._exit_if_bento_still_running()

        cmd = "rm -rf {bento_dir}; tar -zxvf {bento_tar}".format(
            bento_dir=self.bento_home,
            bento_tar=self.bento_tgz)
        print(run(cmd))

        for command_suffix in ["-env.sh", ""]:

            kiji_env = os.path.join(self.bento_home, "bin", "kiji" + command_suffix)
            bento_env = os.path.join(self.bento_home, "bin", "bento" + command_suffix)
            if not os.path.isfile(kiji_env):
                assert os.path.isfile(bento_env)
                cmd = 'cp {bento_env} {kiji_env}'.format(
                    bento_env=bento_env,
                    kiji_env=kiji_env)
                run(cmd)

        cmd = "cd {bento_dir}; source bin/kiji-env.sh; bento start".format(
            bento_dir=self.bento_home,
        )
        print(run(cmd))
        assert os.path.isdir(self.bento_home)

    def _run_express_job(self, class_name, options=""):
        """
        Run any express job.  Handles a lot of boilerplate for all of the Directv jobs (specifying
        dates, kiji table, etc.
        """
        cmd = "source {bento_home}/bin/kiji-env.sh; express job {jar} {myclass} --kiji {kiji_uri}"
        cmd = cmd.format(
            bento_home=self.bento_home,
            jar=os.path.join(self.movie_advisor_home, self.express_jar),
            myclass=class_name,
            kiji_uri=self.kiji_uri,
        ) + " " + options
        print(run(cmd))

    def _run_kiji_job(self, cmd):
        cmd = "source {bento_home}/bin/kiji-env.sh; {cmd}".format(
            bento_home=self.bento_home, cmd=cmd)
        print(run(cmd))

    def _scan_table(self, uri):
        """ Scan this table and print out a couple of rows as a sanity check """
        cmd = 'kiji scan {kiji_uri}/{uri} --max-versions=10'.format(
            kiji_uri=self.kiji_uri,
            uri=uri)
        self._run_kiji_job(cmd)

    def _do_action_tables_create(self):
        """ Run the schema shell to create the tables """

        schema_shell = os.path.join(self.bento_home, "schema-shell", "bin", "kiji-schema-shell")
        assert os.path.isfile(schema_shell), schema_shell

        # Delete the table first!
        cmd = (
            "kiji delete --target={kiji_uri} --interactive=false; " +
            "kiji install --kiji={kiji_uri}" ).format(kiji_uri=self.kiji_uri)
        self._run_kiji_job(cmd)

        for ddl in self.ddls:
            ddl_full_path = os.path.join(self.movie_advisor_home, ddl)
            assert os.path.isfile(ddl_full_path)
            cmd = "{schema_shell} --kiji={kiji_uri} --file={ddl_full_path}".format(
                schema_shell=schema_shell,
                kiji_uri=self.kiji_uri,
                ddl_full_path=ddl_full_path)
            self._run_kiji_job(cmd)

    def _do_action_calculate_similarity_cosine_express(self):
        """ Run the cosine similarity calculator with the appropriate time range. """
        self._run_express_job("com.directv.recommend.express.CosineCFTrainer")
        self._scan_table("content/item_item_similarities")

    def _do_action_import_ratings(self):
        """ Import the movie ratings with an Express job. """
        self._run_express_job(
            "org.kiji.tutorial.load.MovieRatingsImporter",
            options="--ratings ml-100k/u.data"
        )
        self._scan_table("users")

    def _do_action_import_user_info(self):
        """ Import the user metadata with an Express job. """
        self._run_express_job(
            "org.kiji.tutorial.load.UserInfoImporter",
            options="--user-info ml-100k/u.user"
        )
        self._scan_table("users")

    def _do_action_import_movie_info(self):
        """ Import the movie metadata with an Express job. """
        self._run_express_job(
            "org.kiji.tutorial.load.MovieInfoImporter",
            options="--movie-info ml-100k/u.item"
        )
        self._scan_table("movies")

    def _do_action_train(self):
        """ Import the movie metadata with an Express job. """
        self._run_express_job(
            "org.kiji.tutorial.train.ItemSimilarityCalculator"
        )
        self._scan_table("movies")

    def _run_actions(self):
        """ Run whatever actions the user has specified """

        if "install-bento" in self.actions:
            self._do_action_bento_setup()

        if "create-tables" in self.actions:
            self._do_action_tables_create()

        if "import-ratings" in self.actions:
            self._do_action_import_ratings()

        if "import-user-info" in self.actions:
            self._do_action_import_user_info()

        if "import-movie-info" in self.actions:
            self._do_action_import_movie_info()

        if "train-item-item-cf" in self.actions:
            self._do_action_train()

    def go(self, args):
        self._parse_options(args)
        self._run_actions()


if __name__ == "__main__":
    MovieAdvisorManager().go(sys.argv[1:])
