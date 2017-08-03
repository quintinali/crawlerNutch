package pd.nutch.lite.algorithms.ranked.unsupervised.cvalue;

/*
 * Copyright (C) 2014 Angel Conde Manjon neuw84 at gmail.com
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * Represents a Cvalue candidate term
 *
 * @author Angel Conde Manjon
 */
public class Candidate {

  private String text;
  private int lenght;
  private int freq, uniqNesters, freqNested;
  private float cvalue;

  /**
   *
   * @param candidate
   * @param lenght
   */
  public Candidate(String candidate, int lenght) {
    this.text = candidate;
    this.lenght = lenght;
    this.freq = 0;
    this.uniqNesters = 0;
    cvalue = -1;
  }

  /**
   * @return the text
   */
  public String getText() {
    return text;
  }

  /**
   * @param text
   *          the text to set
   */
  public void setText(String text) {
    this.text = text;
  }

  /**
   * @return the lenght
   */
  public int getLenght() {
    return lenght;
  }

  /**
   * @param lenght
   *          the lenght to set
   */
  public void setLenght(int lenght) {
    this.lenght = lenght;
  }

  /**
   *
   * @return
   */
  public float getCValue() {
    if (cvalue == -1) {
      float log_2_lenD = ((float) (Math.log((float) lenght)) / (float) Math.log((float) 2));
      float freqD = (float) freq;
      float invUniqNestersD = 1f / (float) uniqNesters;
      float freqNestedD = (float) freqNested;

      if (uniqNesters == 0) {
        cvalue = log_2_lenD * freqD;
        return cvalue;
      } else {
        cvalue = log_2_lenD * (freqD - invUniqNestersD * freqNestedD);
        return cvalue;
      }
    } else {
      return cvalue;
    }
  }

  /**
   *
   * @return
   */
  public int getFrequency() {
    return freq;
  }

  /**
   *
   * @param freq
   */
  public void incrementFreq(int freq) {
    this.freq += freq;
  }

  /**
   *
   * @return
   */
  public int getLength() {
    return lenght;
  }

  /**
   *
   * @return
   */
  public int getNesterCount() {
    return uniqNesters;
  }

  /**
   *
   */
  public void observeNested() {
    uniqNesters++;
  }

  /**
   *
   * @return
   */
  public int getFreqNested() {
    return freqNested;
  }

  /**
   *
   * @param freq
   */
  public void incrementFreqNested(int freq) {
    freqNested += freq;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Candidate) {
      Candidate x = (Candidate) o;
      return x.getText().equalsIgnoreCase(text);
    } else {
      return false;
    }

  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 29 * hash + (this.text != null ? this.text.hashCode() : 0);
    return hash;
  }

  @Override
  public String toString() {
    return text + " " + this.lenght + " " + this.freq + " " + this.freqNested + " " + this.cvalue;
  }
}
