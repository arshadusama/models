package models

case class Group(
                  group_topics: List[GroupTopics],
                  group_city: String,
                  group_state: Option[String],
                  group_country: String,
                  group_id: Int,
                  group_name: String,
                  group_lon: Double,
                  group_urlname: String,
                  group_lat: Double)
