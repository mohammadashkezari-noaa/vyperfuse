import logging
from typing import Optional, Literal, Union
import pyproj as pp
from vyperdatum.db import DB

LOGGER = logging.getLogger("fuse.tran")

class FuseConfig:
    def __init__(self, config_dict: dict):
        self.metadata = config_dict
        self.from_filename = config_dict.get("from_filename", None)
        if not self.from_filename:
            raise ValueError("from_filename field is required in the Fuse metadata dict.")
        self.ELLIPSOID = "ellipsoid"
        self.ELLIPSOID_AUTH_CODE = "ELL_AUTH_CODE"
        self.tblVCRS = {
                        self.ELLIPSOID: {"height(m)": self.ELLIPSOID_AUTH_CODE},
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


    @staticmethod
    def geographic_to_spc(fips_code: int,
                          units: Literal["m", "ft", "us_ft"]) -> pp.CRS:
        """
        Convert a geographic CRS to a State Place CRS using the
        specified FIPS code.
        """
        def sql(fips_code, units, geodetic_code):
            return (f"select * from projected_crs where deprecated=0"
                    f" and name like '%{fips_code}{units}'"
                    f" and geodetic_crs_code={geodetic_code}")

        assert (units in ["m", "ft", "us_ft"]
                ), f"Units must be 'm', 'ft', 'us_ft', not '{units}'"
        if units == "m":
            units = ""
        elif units == "ft":
            units = "_Feet"
        elif units == "us_ft":
            units = "_Ft_US"
        fips_code = int(fips_code)
        geodetic_code = 6318
        df = DB().query(sql(fips_code, units, geodetic_code), dataframe=True)
        if len(df) != 1:
            LOGGER.error(f"State Plane CRS for FIPS {fips_code} in {units}"
                         f" and based on geodetic CRS EPSG:{geodetic_code} "
                         "not found in the database")
            return None
        return pp.CRS(f"{df.iloc[0].auth_name}:{df.iloc[0].code}")

    @staticmethod
    def geographic_to_utm(zone_number: int) -> pp.CRS:
        """
        Convert a geographic CRS to a UTM CRS using the specified zone number.
        Only for NAD83 2011 based projected CRS with units in m.
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
        if zone_number > 60 or zone_number < 0:
            msg = f"UTM zone number {zone_number} is out of range"
            LOGGER.error(msg)
            raise ValueError(msg)
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
            msg = (f"UTM CRS for zone {zone_number} and "
                   f"auth_code: {utm_code} not found")
            LOGGER.error(msg)
            raise ValueError(msg)
        return pro_crs

    def get_horiz_crs(self, prefix: Literal["form", "to"]) -> Optional[pp.CRS]:
        """
        Construct a pyproj horizontal CRS object using the CRS info
        at the metadata dict.

        Parameters
        ----------
        prefix: Literal["form", "to"]
            To get the source CRS set to 'form', and to get the target CRS, set to 'to'.

        Returns
        ----------
        pyproj.CRS
            Returns a pyproj CRS object.
        """
        assert (prefix in ["from", "to"]
                ), f"Prefix must be 'from' or 'to', not '{prefix}'"
        try:
            h_datum = self.metadata[f"{prefix}_horiz_datum"]
            h_frame = self.metadata[f"{prefix}_horiz_frame"]
            h_type = self.metadata[f"{prefix}_horiz_type"]
            h_units = self.metadata[f"{prefix}_horiz_units"]
            h_key = self.metadata[f"{prefix}_horiz_key"]
        except Exception as e:
            msg = (f"Missing CRS parameter in metadata dict for: "
                   f"{self.metadata['from_filename']}; {e}")
            LOGGER.error(msg)
            raise ValueError(msg)

        if not all([h_datum, h_frame, h_units, h_key]):
            msg = (f"Missing required parameters for {prefix}_horizontal"
                   f" CRS for file: {self.metadata['from_filename']}")
            LOGGER.error(msg)
            raise ValueError(msg)
        try:
            h_crs = pp.CRS(h_frame)
            if h_type.lower() == "utm":
                # UTM always in m
                h_crs = self.geographic_to_utm(zone_number=h_key)
            elif h_type.lower() == "spc":
                h_crs = self.geographic_to_spc(fips_code=h_key, units=h_units)
        except pp.exceptions.CRSError as e:
            LOGGER.error(f"PROJ error creating CRS from {h_frame}, "
                         f"for file: {self.metadata['from_filename']}.\nError message: {e}")
        return h_crs

    def get_vertical_crs(self,
                         prefix: Literal["form", "to"]
                         ) -> Optional[Union[pp.CRS, str]]:
        """
        Construct a pyproj vertical CRS object using the CRS info
        at the config file.

        Parameters
        ----------
        prefix: Literal['form', 'to']
            To get the source CRS set to 'form', and to get the target CRS, set to 'to'.

        Returns
        ----------
        pyproj.CRS
            Returns a pyproj CRS object.
            For Ellipsoid-based vertical CRS, returns string `self.ELLIPSOID_AUTH_CODE`.
            Returns None if the vertical CRS cannot be constructed.
        """
        assert (prefix in ["from", "to"]
                ), f"Prefix must be 'from' or 'to', not '{prefix}'"
        try:
            v_key = self.metadata[f"{prefix}_vert_key"]
            v_units = self.metadata[f"{prefix}_vert_units"]
            v_direction = self.metadata[f"{prefix}_vert_direction"]
        except Exception as e:
            msg = (f"Missing CRS parameter in metadata dict for: "
                   f"{self.metadata['from_filename']}; {e}")
            LOGGER.error(msg)
            return None

        if not all([v_key, v_units, v_direction]):
            msg = (f"Missing required parameters for {prefix}_vertical"
                   f" CRS for file: {self.metadata['from_filename']}")
            LOGGER.error(msg)
            raise ValueError(msg)
        try:
            v_auth_code = self.vertical_crs_look_up(v_key,
                                                    v_direction,
                                                    v_units)
            if v_auth_code == self.ELLIPSOID_AUTH_CODE:
                v_crs = self.ELLIPSOID_AUTH_CODE
            else:
                v_crs = pp.CRS(v_auth_code)
        except pp.exceptions.CRSError as e:
            msg = (f"PROJ error creating vertical CRS from {v_key}, {v_direction},"
                   f"  {v_units} for file: {self.metadata['from_filename']}. The generated "
                   f"auth:code is {v_auth_code}. \nPypoj error message: {e}")
            LOGGER.error(msg)
            return None
        return v_crs

    def ellipsoid_key(self, v_datum: str) -> str:
        """
        Return the ellipsoid datum name that the vertical datum
        refers to the Ellipsoid. Otherwise, return the input
        vertical datum name.
        """
        ell = ("NAD27, NAD83_1986, NAD83_2011, NAD83_NSRS2007, NAD83_MARP00,"
               "NAD83_PACP00, WGS84_G1674, ITRF2014, IGS14, ITRF2008, IGS08,"
               "ITRF2005, IGS2005, WGS84_G1150, ITRF2000, IGS00, IGb00,"
               "WGS84_G873, ITRF94, ITRF93, ITRF92, SIOMIT92, WGS84_G730,"
               "ITRF91, ITRF90, ITRF89, ITRF88, ITRF96, WGS84_TRANSIT,"
               "WGS84_G1762")
        ell = [e.strip() for e in ell.split(",")]
        return v_datum if v_datum.upper() not in ell else self.ELLIPSOID

    def vertical_crs_look_up(self, v_datum: str,
                             direction: Literal["height", "depth"],
                             units: Literal["m", "ftUS"]
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
        v_datum = self.ellipsoid_key(v_datum)
        v_datum = v_datum.lower()
        direction = direction.lower()
        units = units.lower()
        ac = self.tblVCRS.get(v_datum, {}).get(f"{direction}({units})")
        if not ac:
            LOGGER.error(f"Unknown vertical datum '{v_datum}' with "
                         f"direction '{direction}' and units '{units}'")
            return None
        return ac


def get_crs_from_fuse_config(metadata: dict) -> dict:
    """
    Extract the source and target CRS from the provided Fuse metadata dict.

    Parameters
    ----------
    metadata: dict
        A dictionary containing the Fuse metadata.

    Returns
    -------
    Dict
        A dictionary containing the source and target CRS information.
        example: {'from': 'EPSG:6347+EPSG:5703',
                  'to': 'EPSG:6347+NOAA:98',
                  'from_wkt': 'WKT representation of the source CRS',
                  'to_wkt': 'WKT representation of the target CRS'}
    """
    def validate_crs(auth_code: str) -> Optional[str]:
        """
        Validate if the CRS is a valid pyproj CRS.
        """
        try:
            INVALID = None
            invalid_msg = ("The constructed CRS authority code is: "
                           f"'{auth_code}', which is invalid.")
            if not auth_code or not pp.CRS(auth_code):
                LOGGER.error(invalid_msg)
                raise ValueError(invalid_msg)
            return auth_code
        except pp.exceptions.CRSError as e:
            LOGGER.error(f"{invalid_msg}\nInvalid CRS: {e}")
            return INVALID

    fc = FuseConfig(metadata)

    from_h = validate_crs(":".join(fc.get_horiz_crs("from").to_authority()))
    from_vcrs = fc.get_vertical_crs("from")
    if isinstance(from_vcrs, str) and from_vcrs == fc.ELLIPSOID_AUTH_CODE:
        from_crs = from_h
    else:
        from_crs = from_h + "+" + validate_crs(":".join(from_vcrs.to_authority()))

    to_h = validate_crs(":".join(fc.get_horiz_crs("to").to_authority()))
    to_vcrs = fc.get_vertical_crs("to")
    if isinstance(to_vcrs, str) and to_vcrs == fc.ELLIPSOID_AUTH_CODE:
        to_crs = to_h
    else:
        to_crs = to_h + "+" + validate_crs(":".join(to_vcrs.to_authority()))

    from_crs = validate_crs(from_crs)
    to_crs = validate_crs(to_crs)
    crs_dict = {
        "from": from_crs,
        "to": to_crs,
        "from_wkt": pp.CRS(from_crs).to_wkt(),
        "to_wkt": pp.CRS(to_crs).to_wkt()
    }
    return crs_dict





small_metadata = {
    'from_filename': "dummy_filename",
    'from_path': "dummy_path",

    'from_horiz_datum': 'WGS84 geo sounding transformed to NAD83 utm height in reader',
    'from_horiz_frame': "NAD83",
    'from_horiz_type': "utm",
    'from_horiz_key': 18,
    'from_horiz_units': "m",
    'from_vert_datum': "Mean Lower Low Water",
    'from_vert_key': "mllw",
    'from_vert_units': 'm',
    'from_vert_direction': 'height',

    'to_horiz_datum': "NAD83 UTM",
    'to_horiz_frame': "NAD83", 
    'to_horiz_type': "utm", 
    'to_horiz_units': 'm', 
    'to_horiz_key': 18, 
    'to_vert_datum': 'Mean Lower Low Water', 
    'to_vert_key': 'mllw', 
    'to_vert_units': 'm', 
    'to_vert_direction': 'height', 
}

crs_dict = get_crs_from_fuse_config(small_metadata)
print(crs_dict)