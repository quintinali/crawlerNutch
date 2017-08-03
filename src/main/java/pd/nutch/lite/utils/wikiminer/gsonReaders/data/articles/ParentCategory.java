
package pd.nutch.lite.utils.wikiminer.gsonReaders.data.articles;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Generated;

/**
 *
 * @author angel
 */
@Generated("com.googlecode.jsonschema2pojo")
public class ParentCategory implements Serializable {
  private static final long serialVersionUID = 1L;

  private Integer id;
  private String title;
  private final Map<String, Object> additionalProperties = new HashMap<>();

  /**
   *
   * @return
   */
  public Integer getId() {
    return id;
  }

  /**
   *
   * @param id
   */
  public void setId(Integer id) {
    this.id = id;
  }

  /**
   *
   * @return
   */
  public String getTitle() {
    return title;
  }

  /**
   *
   * @param title
   */
  public void setTitle(String title) {
    this.title = title;
  }

  /**
   *
   * @return
   */
  public Map<String, Object> getAdditionalProperties() {
    return this.additionalProperties;
  }

  /**
   *
   * @param name
   * @param value
   */
  public void setAdditionalProperties(String name, Object value) {
    this.additionalProperties.put(name, value);
  }

}
