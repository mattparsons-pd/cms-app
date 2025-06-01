from config import Configuration
from utils.process import Process

my_env = Configuration()
process = Process(
    url=my_env.CMS_DATASTORE,
    theme="Hospitals",
    output_dir=my_env.OUTPUT_DIR,
    max_workers=my_env.MAX_WORKERS
)


process.run()