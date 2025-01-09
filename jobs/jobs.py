from nautobot.apps import jobs

name = "My Group Of Jobs"  # optional, but recommended to define a grouping name


class MyNewJob(jobs.Job):
    class Meta:
        # metadata attributes go here
        name = "My New Job"

    # input variable definitions go here
    some_text_input = jobs.StringVar(required=True)

    def run(self, some_text_input):
        # code to execute when the Job is run goes here
        self.logger.info("some_text_input: %s", some_text_input)


jobs.register_jobs(MyNewJob)
