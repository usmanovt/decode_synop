# -*- coding: utf-8 -*-
"""
Created on Sat May  6 14:54:39 2023

@author: admin
"""
"""synop object for decoding SYNOP reports."""
import re
import logging
import numpy as np
from collections import OrderedDict

from handlers import (default_handler, handle_MMMM, handle_wind_unit, handle_iihVV,
                      handle_Nddff, handle_sTTT, handle_PPPP, handle_5appp,
                      handle_6RRRt, handle_7wwWW, handle_8NCCC, handle_3EsTT, handle_5EsTT,
                      handle_4Esss, handle_55SSS, handle_7RRR, handle_8NChh)

_logger = logging.getLogger(__name__)

#regex definitions
#split report into its sections
sections_re = re.compile(r"""(?P<section_0>AAXX\s+[\d]{5}\s+[\d]{5})\s+
                             ((?P<section_1>((\d|\/){5}\s+){0,10})){0,1}
                             ((333\s+)(?P<section_3>((\d|\/){5}\s+){0,8})){0,1}
                             ((555\s+)(?P<section_5>((\d|\/){5}\s+){0,6})){0,1}""",
                             re.VERBOSE)

#split section 0
section_0_re = re.compile(r"""(?P<MMMM>AAXX)\s+
                             (?P<monthdayr>[\d]{2})
                             (?P<hourr>[\d]{2})
                             (?P<wind_unit>[\d]{1})\s+
                             (?P<station_id>[\d]{5})""", re.VERBOSE)


#split section 1
section_1_re = re.compile(r"""((?P<iihVV>(\d|\/){5})\s+
                              (?P<Nddff>(\d|/){5})\s+
                              (00(?P<fff>\d{3})\s+)?
                              (1(?P<t_air>(\d|/){4})\s+)?
                              (2(?P<dewp>(\d|/){4})\s+)?
                              (3(?P<p_baro>(\d\d\d\d|\d\d\d\/))\s+)?
                              (4(?P<p_slv>(\d\d\d\d|\d\d\d\/))\s+)?
                              (5(?P<appp>\d{4})\s+)?
                              (6(?P<RRRt>(\d|/){3}\d\s+))?
                              (7(?P<wwWW>\d{2}(\d|/)(\d|/))\s+)?
                              (8(?P<NCCC>(\d|/){4})\s+)?
                              (9(?P<GGgg>\d{4})\s+)?)?""",
                              re.VERBOSE)

s1_iihVV_re = re.compile(r"""((?P<ir>\d)(?P<ix>\d)(?P<h>(\d|\/))(?P<VV>(\d|\/){2}))?""", re.VERBOSE)
s1_Nddff_re = re.compile(r"""((?P<N>(\d|/))(?P<dd>\d\d)(?P<ff>\d\d))?""", re.VERBOSE)
s1_00fff_re = re.compile(r"""((?P<wind_speed>\d{3}))?""", re.VERBOSE)
#s1_1sTTT_re = re.compile(r"""(?P<air_t>\d{4})""", re.VERBOSE)
#s1_2sTTT_re = re.compile(r"""(?P<dewp>\d{4})""", re.VERBOSE)
#s1_3PPPP_re = re.compile(r"""(?P<p_baro>.*)""", re.VERBOSE)
#s1_4PPPP_re = re.compile(r"""(?P<p_slv>.*)""", re.VERBOSE)
s1_5appp_re = re.compile(r"""((?P<a>\d)(?P<ppp>\d{3}))?""", re.VERBOSE)
s1_6RRRt_re = re.compile(r"""((?P<RRR>\d{3})(?P<t>(\d|/)))?""", re.VERBOSE)
s1_7wwWW_re = re.compile(r"""((?P<ww>\d{2})(?P<W1>\d)(?P<W2>\d))?""", re.VERBOSE)
s1_8NCCC_re = re.compile(r"""((?P<N>\d)(?P<CL>(\d|/))(?P<CM>(\d|/))(?P<CH>(\d|/)))?""", re.VERBOSE)
s1_9GGgg_re = re.compile(r"""((?P<observation_time>.*))?""", re.VERBOSE)

#split section 3
section_3_re = re.compile(r"""(1(?P<t_max>\d{4}\s+))?
                              (2(?P<t_min>\d{4}\s+))?
                              (3(?P<EsTT>\d{4}\s+))?
                              (4(?P<Esss>(\d|/)\d{3}\s+))?
                              (55(?P<SSS>(\d|/){3}\s+))?
                              (6(?P<RRRt>(\d\d\d|///)\d\s+))?
                              (8(?P<NChh>(\d(\d|/)\d\d\s+)))?
                              (9(?P<SSss_1>\d{4}\s+))?
                              (9(?P<SSss_2>\d{4}\s+))?
                              (9(?P<SSss_3>\d{4}\s+))?""",
                              re.VERBOSE)

s3_EsTT_re = re.compile(r"""((?P<E>\d)(?P<sTT>\d{3}))?""", re.VERBOSE)
s3_Esss_re = re.compile(r"""((?P<E>(\d|/))(?P<sss>\d{3}))?""", re.VERBOSE)
s3_55SSS_re = re.compile(r"""(55(?P<SSS>\d{3}\s+)?)?""", re.VERBOSE)
s3_8NChh_re = re.compile(r"""((8(?P<c1>\d(\d|/)\d\d)\s+)?
                             (8(?P<c2>\d(\d|/)\d\d)\s+)?
                             (8(?P<c3>\d(\d|/)\d\d)\s+)?
                             (8(?P<c4>\d(\d|/)\d\d)\s+)?)?""",
                             re.VERBOSE)

#split section 5
section_5_re = re.compile(r"""(1(?P<EsTT_1>\d{4}\s+))?
                          (2(?P<t_min>\d{4}\s+))?
                          (3(?P<EsTT_3>\d{4}\s+))?
                          (6(?P<RRRt>(\d|/){3}\d\s+))?
                          (7(?P<RRR_24>\d{3})\/\s+)?
                          """, re.VERBOSE)

s5_EsTT_1_re = re.compile(r"""((?P<E>\d)(?P<sTT>\d{3}))?""", re.VERBOSE)
s5_EsTT_3_re = re.compile(r"""((?P<E>\d)(?P<sTT>\d{3}))?""", re.VERBOSE)
s5_RRR_re = re.compile(r"""((?P<RRR_24>\d{3}))?""", re.VERBOSE)


def _report_match(handler, match):
    """Report success or failure of the given handler function. (DEBUG)."""
    if match:
        _logger.debug("%s matched '%s'", handler.__name__, match)
    else:
        _logger.debug("%s didn't match...", handler.__name__)


def missing_value(f):
    """Missing value decorator."""
    def decorated(*args, **kwargs):
        if args[1] is None:
            return np.nan
        else:
            return f(*args, **kwargs)
    return decorated


class synop(object):
    """SYNOP report."""
    
    def __init__(self, report):
        """Decode SYNOP report.
        Parameters
        ----------
        report : str
            Raw SYNOP report
        """
        self.raw = report
        self.decoded = None
        self.type = "SYNOP"
        self.datetime = None
        self.station_id = None

        #decoded is a dict of dicts in form {"section_x": {"group_name or variable": value}}
        self.decoded = sections_re.match(self.raw).groupdict("")
        #split raw report into its sections then split each section into
        #its groups and handle (decode) each group
        #use sorted to make sure report is decoded starting with section 0
        for sname in sorted(self.decoded.keys()):
            sraw = self.decoded[sname]
            pattern, ghandlers = self.handlers[sname]
            #TODO
            #- add try except for matching and collect string when  match is empty
            #sec_groups = patter.match(sraw).groupdict()
            #self.decoded[sname] = pattern.match(sraw).groupdict()
            gd = pattern.match(sraw).groupdict("")
            #try:
                #gd = pattern.match(sraw).groupdict("")
            #except:
                #print(self.raw)
                #print(self.decoded)
                #print(sname, sraw)

            #if section is not none create dictionary for it
            self.decoded[sname] = {}
            for gname, graw in gd.items():
                if gname not in ghandlers:
                    continue
                gpattern, ghandler = ghandlers[gname]
                #if the group can be decoded directly without further regex pattern
                #handle it directly otherwise match it against a group pattern
                if gpattern is None:
                    self.decoded[sname][gname] = ghandler(graw)
                else:
                    group = gpattern.match(graw)
                    #_report_match(ghandler, group.group())
                    #self.decoded[sname][gname] = ghandler(self, group.groupdict())
                    self.decoded[sname].update(ghandler(group.groupdict("")))

    #format of the handlers is (group_regex_pattern, handler)
    #if group regex pattern is None the group can be directly decoded e.g. a single variable in a group
    #otherwise a pattern is used to split the group using regex so the handler can access each variable
    #from a dictionary
    sec0_handlers = (section_0_re,
                     {"datetime": (None, default_handler),
                      "MMMM": (None, handle_MMMM),
                      "monthdayr": (None, default_handler),
                      "hourr": (None, default_handler),
                      "wind_unit": (None, handle_wind_unit),
                      "station_id": (None, default_handler)
                      })

    sec1_handlers = (section_1_re,
                     {"iihVV": (s1_iihVV_re, handle_iihVV),
                      "Nddff": (s1_Nddff_re, handle_Nddff),
                      # "fff": (s1_00fff_re, handle_00fff),
                      "t_air": (None, handle_sTTT),
                      "dewp": (None, handle_sTTT),
                      "p_baro": (None, handle_PPPP),
                      "p_slv": (None, handle_PPPP),
                      "appp": (s1_5appp_re, handle_5appp),
                      "RRRt": (s1_6RRRt_re, handle_6RRRt),
                      "wwWW": (s1_7wwWW_re, handle_7wwWW),
                      "NCCC": (s1_8NCCC_re, handle_8NCCC),
                      # "GGgg": (s1_9GGgg_re, handle_9GGgg),
                      })

    sec3_handlers = (section_3_re,
                     {"t_max": (None, handle_sTTT),
                      "t_min": (None, handle_sTTT),
                      # "EsTT": (s3_EsTT_re, handle_3EsTT),
                      "Esss": (s3_Esss_re, handle_4Esss),
                      "SSS": (None, handle_55SSS),
                      "RRRt": (s1_6RRRt_re, handle_6RRRt),
                      "NChh": (s3_8NChh_re, handle_8NChh),
                      "SSss_1": (None, default_handler),
                      "SSss_2": (None, default_handler),
                      "SSss_3": (None, default_handler),
                      })

    sec5_handlers = (section_5_re,
                     {"EsTT_1": (s5_EsTT_1_re, handle_3EsTT),
                      "t_min": (None, handle_sTTT),
                      "EsTT_3": (s5_EsTT_3_re, handle_5EsTT),
                      "RRRt": (s1_6RRRt_re, handle_6RRRt),
                      "RRR_24": (None, handle_7RRR),
                      })


    handlers = {"section_0": sec0_handlers,
                "section_1": sec1_handlers,
                "section_3": sec3_handlers,
                "section_5": sec5_handlers
                }

    def __str__(self):
        def prettydict(d, indent=0):
            """Print dict (of dict) pretty with indent.
            Parameters
            ----------
            d : dict
            Returns
            -------
            print
            """
            for key, value in d.items():
                if isinstance(value, dict):
                    print("\t" * indent + str(key) + ":")
                    prettydict(value, indent + 1)
                else:
                    #print("\t" * (indent + 1) + str(value))
                    print("\t" * indent + str(key) + ": " + str(value))
            return

        prettydict(self.decoded)

        return

    def convert_units(self):
        """Convert units."""
        #convert units if necessary
        #use unit indicator of section_0
        w_unit = self.decoded["section_0"]["wind_unit"]
        wind_speed = self.decoded["section_1"]["wind_speed"]
        knots_to_mps_factor = 0.51444444444444
        if w_unit in ["knots estimate", "knots measured"]:
            wind_speed = wind_speed * knots_to_mps_factor
        else:
            return

        if "estimate" in w_unit:
            new_wind_unit = "meters per second estimate"
        else:
            new_wind_unit = "meters per second measured"

        self.decoded["section_0"]["wind_unit"] = new_wind_unit
        self.decoded["section_1"]["wind_speed"] = wind_speed

    def to_dict(self, vars=None):
        """Convert selected variables of report to a pandas dataframe.
        Parameters
        ----------
        vars : list of str
            List of variables to include
        Returns
        -------
        dict
        """
        if vars is None:
            vars = self.decoded.keys()

        vardict = OrderedDict.fromkeys(vars)

        # self.decoded is dict of dicts with sections as keys
        for i in self.decoded.values():
            if i is not None:
                tmp = {k:v for k, v in i.items() if k in vars}
                vardict.update(tmp)

        return vardict
                             