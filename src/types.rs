//! Rust Types.

use std::io::prelude::*;
use std::convert::From;
use diesel::deserialize::{self, FromSql};
use diesel::serialize::{self, IsNull, Output, ToSql};
use diesel::pg::Pg;
use postgis::ewkb::*;
use crate::sql_types::*;

#[derive(Debug, Copy, Clone, PartialEq, FromSqlRow, AsExpression)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[sql_type = "Geography"]
pub struct GeogPoint {
	pub x: f64, // lon
	pub y: f64, // lat
	pub srid: Option<i32>,
}

impl From<Point> for GeogPoint {
	fn from(p: Point) -> Self {
		let Point { x, y, srid } = p;
		Self { x, y, srid }
	}
}
impl From<GeogPoint> for Point {
	fn from(p: GeogPoint) -> Self {
		let GeogPoint { x, y, srid } = p;
		Self { x, y, srid }
	}
}

impl FromSql<Geography, Pg> for GeogPoint {
	fn from_sql(bytes: Option<&[u8]>) -> deserialize::Result<Self> {
		use std::io::Cursor;
		use postgis::ewkb::EwkbRead;
		let bytes = not_none!(bytes);
		let mut rdr = Cursor::new(bytes);
		Ok(Point::read_ewkb(&mut rdr)?.into())
	}
}

impl ToSql<Geography, Pg> for GeogPoint {
	fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
		use postgis::ewkb::{AsEwkbPoint, EwkbWrite};
		Point::from(*self).as_ewkb().write_ewkb(out)?;
		Ok(IsNull::No)
	}
}


#[derive(Debug, Copy, Clone, PartialEq, FromSqlRow, AsExpression)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[sql_type = "Geography"]
pub struct GeogPointZ {
	pub x: f64, // lon
	pub y: f64, // lat
	pub z: f64, // z
	pub srid: Option<i32>,
}

impl From<PointZ> for GeogPointZ {
	fn from(p: PointZ) -> Self {
		let PointZ { x, y, z, srid } = p;
		Self { x, y, z, srid }
	}
}
impl From<GeogPointZ> for PointZ {
	fn from(p: GeogPointZ) -> Self {
		let GeogPointZ { x, y, z, srid } = p;
		Self { x, y, z, srid }
	}
}

impl FromSql<Geography, Pg> for GeogPointZ {
	fn from_sql(bytes: Option<&[u8]>) -> deserialize::Result<Self> {
		use std::io::Cursor;
		use postgis::ewkb::EwkbRead;
		let bytes = not_none!(bytes);
		let mut rdr = Cursor::new(bytes);
		Ok(PointZ::read_ewkb(&mut rdr)?.into())
	}
}

impl ToSql<Geography, Pg> for GeogPointZ {
	fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
		use postgis::ewkb::{AsEwkbPoint, EwkbWrite};
		PointZ::from(*self).as_ewkb().write_ewkb(out)?;
		Ok(IsNull::No)
	}
}


#[derive(Debug, Clone, PartialEq, FromSqlRow, AsExpression)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[sql_type = "Geography"]
pub struct GeogLineString {
	pub points: Vec<GeogPoint>,
	pub srid: Option<i32>,
}

impl From<LineString> for GeogLineString {
	fn from(p: LineString) -> Self {
		let LineString { points, srid } = p;

		// TODO: Can we cast memory inplace?
		let convertedPoints = points
			.iter()
			.map(|p| {
				GeogPoint {
					x: p.x,
					y: p.y,
					srid: srid,
				}
			})
			.collect::<Vec<GeogPoint>>();

		Self { points: convertedPoints, srid }
	}
}
impl From<GeogLineString> for LineString {
	fn from(p: GeogLineString) -> Self {
		let GeogLineString { points, srid } = p;

		let convertedPoints = points
			.iter()
			.map(|p| {
				Point {
					x: p.x,
					y: p.y,
					srid: srid,
				}
			})
			.collect::<Vec<Point>>();

		Self { points: convertedPoints, srid }
	}
}

impl FromSql<Geography, Pg> for GeogLineString {
	fn from_sql(bytes: Option<&[u8]>) -> deserialize::Result<Self> {
		use std::io::Cursor;
		use postgis::ewkb::EwkbRead;
		let bytes = not_none!(bytes);
		let mut rdr = Cursor::new(bytes);
		Ok(LineString::read_ewkb(&mut rdr)?.into())
	}
}

impl ToSql<Geography, Pg> for GeogLineString {
	fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
		use postgis::ewkb::{AsEwkbPoint, EwkbWrite};
		LineString::from(self.clone()).as_ewkb().write_ewkb(out)?;
		Ok(IsNull::No)
	}
}

#[derive(Debug, Clone, PartialEq, FromSqlRow, AsExpression)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[sql_type = "Geography"]
pub struct GeogLineStringZ {
	pub points: Vec<GeogPointZ>,
	pub srid: Option<i32>,
}

impl From<LineStringZ> for GeogLineStringZ {
	fn from(p: LineStringZ) -> Self {
		let LineStringZ { points, srid } = p;

		// TODO: Can we cast memory inplace?
		let convertedPoints = points
			.iter()
			.map(|p| {
				GeogPointZ {
					x: p.x,
					y: p.y,
					z: p.z,
					srid: srid,
				}
			})
			.collect::<Vec<GeogPointZ>>();

		Self { points: convertedPoints, srid }
	}
}
impl From<GeogLineStringZ> for LineStringZ {
	fn from(p: GeogLineStringZ) -> Self {
		let GeogLineStringZ { points, srid } = p;

		// TODO: Can we cast memory inplace?
		let convertedPoints = points
			.iter()
			.map(|p| {
				PointZ {
					x: p.x,
					y: p.y,
					z: p.z,
					srid: srid,
				}
			})
			.collect::<Vec<PointZ>>();

		Self { points: convertedPoints, srid }
	}
}

impl FromSql<Geography, Pg> for GeogLineStringZ {
	fn from_sql(bytes: Option<&[u8]>) -> deserialize::Result<Self> {
		use std::io::Cursor;
		use postgis::ewkb::EwkbRead;
		let bytes = not_none!(bytes);
		let mut rdr = Cursor::new(bytes);
		Ok(LineStringZ::read_ewkb(&mut rdr)?.into())
	}
}

impl ToSql<Geography, Pg> for GeogLineStringZ {
	fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
		use postgis::ewkb::{AsEwkbPoint, EwkbWrite};
		LineStringZ::from(self.clone()).as_ewkb().write_ewkb(out)?;
		Ok(IsNull::No)
	}
}

#[derive(Debug, Clone, PartialEq, FromSqlRow, AsExpression)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[sql_type = "Geography"]
pub struct GeogPolygon {
	pub rings: Vec<GeogLineString>,
	pub srid: Option<i32>,
}

impl From<Polygon> for GeogPolygon {
	fn from(p: Polygon) -> Self {
		let Polygon { rings, srid } = p;

		// TODO: Can we cast memory inplace?
		let mut convertedLines: Vec<GeogLineString> = Vec::new();
		for line in &rings
		{
			let convertedPoints = line
				.points
				.iter()
				.map(|p| {
					GeogPoint {
						x: p.x,
						y: p.y,
						srid: srid,
					}
				})
				.collect::<Vec<GeogPoint>>();
			convertedLines.push(GeogLineString{ points: convertedPoints, srid: line.srid });
		}

		Self { rings: convertedLines, srid }
	}
}
impl From<GeogPolygon> for Polygon {
	fn from(p: GeogPolygon) -> Self {
		let GeogPolygon { rings, srid } = p;

		// TODO: Can we cast memory inplace?
		let mut convertedLines: Vec<LineString> = Vec::new();
		for line in &rings
		{
			let convertedPoints = line
				.points
				.iter()
				.map(|p| {
					Point {
						x: p.x,
						y: p.y,
						srid: srid,
					}
				})
				.collect::<Vec<Point>>();
			convertedLines.push(LineString{ points: convertedPoints, srid: line.srid });
		}

		Self { rings: convertedLines, srid }
	}
}

impl FromSql<Geography, Pg> for GeogPolygon {
	fn from_sql(bytes: Option<&[u8]>) -> deserialize::Result<Self> {
		use std::io::Cursor;
		use postgis::ewkb::EwkbRead;
		let bytes = not_none!(bytes);
		let mut rdr = Cursor::new(bytes);
		Ok(Polygon::read_ewkb(&mut rdr)?.into())
	}
}

impl ToSql<Geography, Pg> for GeogPolygon {
	fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
		use postgis::ewkb::{AsEwkbPoint, EwkbWrite};
		Polygon::from(self.clone()).as_ewkb().write_ewkb(out)?;
		Ok(IsNull::No)
	}
}
