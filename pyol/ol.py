from subprocess import call
from pyol.utils import logger

class OL():
    def __init__(self, executable_path=None):
        self.executable_path = executable_path or self.find_executable()
        pass

    def find_executable():
        """Try `which ol`. If fails, prompt the user to enter the path of open lambda."""
        if call(["which", "ol"]) == 0:
            return "ol"
        raise Exception("Please specify the path to openlambda executable (usually `ol`) in $PATH.")

    def new(path=None):
        pass
        
    def worker():
        pass

    def status():
        pass

    def kill():
        logger.info("Kill worker")
        pass


default_ol = OL("ol")

def setup_ol(ol_path):
    """Setup a new ol object, possibly pointing to a different executable."""
    ol = OL(ol_path)
    return OL(ol_path)