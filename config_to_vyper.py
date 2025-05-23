import logging
from typing import Optional, Literal
import configparser
import pyproj as pp
from vyperdatum.db import DB

LOGGER = logging.getLogger("fuse.tran")

class MetaFuse:
    def __init__(self, config_file):
        self.config_file = config_file
        self.config = configparser.ConfigParser()
        self.config.read(self.config_file)
        self.tblVCRS = {
                        "navd88": {"height(m)": "EPSG:5703",
                                   "depth(m)": "EPSG:6357",
                                   "height(ftus)": "EPSG:6360",
                                   "depth(ftus)": "EPSG:6358"},
                        "ngvd29": {"height(m)": "EPSG:7968",
                                   "height(ftus)": "EPSG:5702",
                                   "depth(ftus)": "EPSG:6359"},
                        "ncd": {"height(m)": "NOAA:101",
                                "depth(m)": "NOAA:100"},
                        "hrd": {"height(m)": "NOAA:86",
                                "depth(m)": "NOAA:66"},
                        "crd": {"height(m)": "NOAA:87",
                                "depth(m)": "NOAA:67"},
                        "lwrp": {"height(m)": "NOAA:89",
                                 "depth(m)": "NOAA:69"},
                        "mlw": {"height(m)": "NOAA:99",
                                "depth(m)": "NOAA:79"},
                        "mllw": {"height(m)": "NOAA:98",
                                 "depth(m)": "NOAA:78"},
                        "igld85": {"height(m)": "NOAA:92",
                                   "depth(m)": "NOAA:72"},
                        "igld85lwd": {"height(m)": "NOAA:93",
                                       "depth(m)": "NOAA:73"},
        }

    def get_config(self, section, option):
        try:
            return self.config.get(section, option)
        except configparser.NoOptionError:
            LOGGER.error(f"Option '{option}' not found in section '{section}'")
            return None
        except configparser.NoSectionError:
            LOGGER.error(f"Section '{section}' not found in the configuration file")
            return None

    @staticmethod
    def geographic_to_utm_by_proj(geographic_crs: pp.CRS,
                                  zone_number: int,
                                  southern_hemisphere=False) -> pp.CRS:
        """
        Convert a geographic CRS to a UTM CRS using the specified zone number.
        """
        datum = "NAD83"  # geographic_crs.datum.name
        ellps = "GRS80"  # geographic_crs.ellipsoid.name
        hemisphere = " +south" if southern_hemisphere else ""
        proj_string = (
            f"+proj=utm +zone={zone_number}{hemisphere} "
            f"+datum={datum} +ellps={ellps} +units=m +no_defs"
        )
        return pp.CRS.from_proj4(proj_string)

    @staticmethod
    def geographic_to_utm_by_db(geographic_crs: pp.CRS,
                                zone_number: int,
                                southern_hemisphere=False) -> pp.CRS:
        """
        Convert a geographic CRS to a UTM CRS using the specified zone number.
        """
        authority, code = geographic_crs.to_authority()
        zone_number = int(zone_number)
        hemi = "S" if southern_hemisphere else "N"
        sql = (f"select * from projected_crs where deprecated=0"
               f" and name like '% {zone_number}{hemi}'"
               f" and geodetic_crs_auth_name='{authority}'"
               f" and geodetic_crs_code={code}")
        df = DB().query(sql, dataframe=True)
        if len(df) != 1:
            LOGGER.error(f"UTM CRS for {authority}:{code} and zone"
                         f" {zone_number}{hemi} not found in the database")
            return None
        return pp.CRS(f"{df.iloc[0].auth_name}:{df.iloc[0].code}")

    @staticmethod
    def geographic_to_utm(zone_number: int) -> pp.CRS:
        """
        Convert a geographic CRS to a UTM CRS using the specified zone number.
        Only for NAD83 2011.
        """
        def nad83_to_utm(zone_number: int) -> pp.CRS:
            """
            Construct a projected UTM CRS based on NAD83 and
            the specified zone number.
            """
            utm_code = f"EPSG:{26900 + zone_number}"
            if zone_number == 59:
                utm_code = "EPSG:3372"
            elif zone_number == 60:
                utm_code = "EPSG:3373"
            return pp.CRS(utm_code), utm_code

        def nad83_2011_to_utm(zone_number: int) -> pp.CRS:
            """
            Construct a projected UTM CRS based on NAD83 2011 and
            the specified zone number.
            """
            utm_code = f"EPSG:{6329 + zone_number}"
            if zone_number == 59:
                utm_code = "EPSG:6328"
            elif zone_number == 60:
                utm_code = "EPSG:6329"
            return pp.CRS(utm_code), utm_code

        zone_number = int(zone_number)
        if zone_number in [54, 55, 58]:
            if zone_number == 54:
                utm_code = "EPSG:8692"
            elif zone_number == 55:
                utm_code = "EPSG:8693"
            elif zone_number == 58:
                utm_code = "ESRI:102213"
            pro_crs = pp.CRS(utm_code)
        else:
            pro_crs, utm_code = nad83_2011_to_utm(zone_number)
            if not pro_crs.utm_zone:
                pro_crs, utm_code = nad83_to_utm(zone_number)

        if not pro_crs.utm_zone:
            LOGGER.error(f"UTM CRS for zone {zone_number} and "
                         f"auth_code: {utm_code} not found")
            return None
        return pro_crs

    def get_horiz_crs(self, prefix: str, section: str = "Default") -> Optional[pp.CRS]:
        """
        Construct a pyproj horizontal CRS object using the CRS info
        at the config file.

        Parameters
        ----------
        prefix: str
            To get the source CRS set to 'form', and to get the target CRS, set to 'to'.
        section: str
            Section header in the config file.

        Returns
        ----------
        pyproj.CRS
            Returns a pyproj CRS object.
        """
        assert (prefix in ["from", "to"]
                ), f"Prefix must be 'from' or 'to', not '{prefix}'"
        assert (section in self.config.sections()
                ), f"Section '{section}' not found in the configuration file"
        # h_datum = self.config.get(section, f"{prefix}_horiz_datum")
        h_frame = self.config.get(section, f"{prefix}_horiz_frame")
        h_type = self.config.get(section, f"{prefix}_horiz_type")
        # h_units = self.config.get(section, f"{prefix}_horiz_units") # UTM always is in m
        h_key = self.config.get(section, f"{prefix}_horiz_key")

        try:
            # h_frame = "NAD83_2011"
            h_crs = pp.CRS(h_frame)
            if h_key and h_type and h_type.lower() == "utm":
                h_crs = self.geographic_to_utm(#geographic_crs=h_crs,
                                               zone_number=h_key,
                                               #southern_hemisphere=False
                                               )
        except pp.exceptions.CRSError as e:
            LOGGER.error(f"Error creating CRS from {h_frame}: {e}")
            return None
        return h_crs

    def get_vertical_crs(self,
                         prefix: Literal["form", "to"],
                         section: str = "Default"
                         ) -> Optional[pp.CRS]:
        """
        Construct a pyproj vertical CRS object using the CRS info
        at the config file.

        Parameters
        ----------
        prefix: Literal['form', 'to']
            To get the source CRS set to 'form', and to get the target CRS, set to 'to'.
        section: str
            Section header in the config file.

        Returns
        ----------
        pyproj.CRS
            Returns a pyproj CRS object.
        """
        assert (prefix in ["from", "to"]
                ), f"Prefix must be 'from' or 'to', not '{prefix}'"
        assert (section in self.config.sections()
                ), f"Section '{section}' not found in the configuration file"
        v_key = self.config.get(section, f"{prefix}_vert_key")
        v_units = self.config.get(section, f"{prefix}_vert_units")
        v_direction = self.config.get(section, f"{prefix}_vert_direction")
        try:
            v_crs = pp.CRS(self.vertical_crs_look_up(v_key, v_direction, v_units))
        except pp.exceptions.CRSError as e:
            LOGGER.error("Error creating vertical CRS from"
                         f" {v_key}, {v_direction}, {v_units}: {e}")
            return None
        return v_crs

    def vertical_crs_look_up(self, v_datum: str,
                             direction: Literal["height", "depth"],
                             units: Literal["m", "ft"]
                             ) -> Optional[str]:
        """
        Return 'Authority:Code' representation of a registered vertical CRS
        according to the provided alias.

        Parameters
        ----------
        v_datum: str
            Vertical datum alias (a la VDATUM Vertical Reference Frames List
            https://vdatum.noaa.gov/docs/services.html).

        direction: Literal['height', 'depth']
            Direction of the vertical datum.

        units: Literal['m', 'ftUS']
            Measurement unit.

        Returns
        ----------
        str
            Returns 'Authority:Code' representation of a registered
            vertical CRS.
        """
        assert isinstance(v_datum, str), f"v_datum must be a string, not {type(v_datum)}"
        assert v_datum, "v_datum must not be empty"
        assert (direction in ["height", "depth"]
                ), f"Direction must be 'height' or 'depth', not '{direction}'"
        assert (units in ["m", "ftUS"]
                ), f"Units must be 'm' or 'ftUS', not '{units}'"
        v_datum = v_datum.lower()
        direction = direction.lower()
        units = units.lower()
        ac = self.tblVCRS.get(v_datum, {}).get(f"{direction}({units})")
        if not ac:
            LOGGER.error(f"Unknown vertical datum '{v_datum}' with direction '{direction}' and units '{units}'")
            return None
        return ac




from glob import glob

files = glob("./Updated_Configs/**/*.config")
print(files)
for f in files:
    print(f)
    if f.find("enc_pbc_northeast_utm19n_mld_enc.config") != -1 or f.find("Unused") != -1:
        continue
    mf = MetaFuse(f)
    # print(mf.get_config("Default", "to_horiz_frame"))
    print(mf.get_config("Default", "to_horiz_key"))
    print(mf.get_horiz_crs("to", "Default").to_authority())
    print(mf.get_vertical_crs("to", "Default").to_authority())
    print("-----------")
