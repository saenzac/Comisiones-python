import logging
from colorama import Fore
import ipywidgets as widgets
from IPython.display import display

class LogViewver(logging.Handler):
    """ Class to redistribute python logging data """
    # have a class member to store the existing logger
    logger_instance = logging.getLogger("juplogger")
    def __init__(self, *args, **kwargs):
        # Initialize the Handler
        logging.Handler.__init__(self, *args)

        #Output widget
        self.output_gui = widgets.Output(layout=widgets.Layout(width='100%', height='250px', border='solid'))

        # optional take format
        # setFormatter function is derived from logging.Handler
        for key, value in kwargs.items():
            if "{}".format(key) == "format":
                self.setFormatter(value)

        # make the logger send data to this class
        self.logger_instance.addHandler(self)

    def emit(self, record):
        """ Overload of logging.Handler method """

        record = self.format(record)
        self.output_gui.outputs = ({'name': 'stdout', 'output_type': 'stream', 'text': (Fore.BLACK + (record + '\n'))},) + self.output_gui.outputs

    def clear_logs(self):
        """ Clear the current logs """
        self.output_gui.clear_output()

    def show_logs(self):
        """ Show the logs """
        display(self.output_gui)