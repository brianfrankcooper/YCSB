/**
 * Copyright (c) 2016-2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.yahoo.ycsb.generator;

import java.util.*;

/**
 * A generator that produces strings of {@link #length} using a set of code points
 * from {@link #characterSet}. Each time {@link #nextValue()} is executed, the string
 * is incremented by one character. Eventually the string may rollover to the beginning
 * and the user may choose to have the generator throw a NoSuchElementException at that 
 * point or continue incrementing. (By default the generator will continue incrementing).
 * <p>
 * For example, if we set a length of 2 characters and the character set includes
 * [A, B] then the generator output will be:
 * <ul>
 * <li>AA</li>
 * <li>AB</li>
 * <li>BA</li>
 * <li>BB</li>
 * <li>AA <-- rolled over</li>
 * </ul>
 * <p>
 * This class includes some default character sets to choose from including ASCII
 * and plane 0 UTF. 
 */
public class IncrementingPrintableStringGenerator extends Generator<String> {

  /** Default string length for the generator. */
  public static final int DEFAULTSTRINGLENGTH = 8;

  /**
   * Set of all character types that include every symbol other than non-printable
   * control characters.
   */
  public static final Set<Integer> CHAR_TYPES_ALL_BUT_CONTROL;

  static {
    CHAR_TYPES_ALL_BUT_CONTROL = new HashSet<Integer>(24);
    // numbers
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.DECIMAL_DIGIT_NUMBER);
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.LETTER_NUMBER);
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.OTHER_NUMBER);

    // letters
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.UPPERCASE_LETTER);
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.LOWERCASE_LETTER);
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.TITLECASE_LETTER);
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.OTHER_LETTER);

    // marks
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.COMBINING_SPACING_MARK);
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.NON_SPACING_MARK);
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.ENCLOSING_MARK);

    // punctuation
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.CONNECTOR_PUNCTUATION);
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.DASH_PUNCTUATION);
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.START_PUNCTUATION);
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.END_PUNCTUATION);
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.INITIAL_QUOTE_PUNCTUATION);
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.FINAL_QUOTE_PUNCTUATION);
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.OTHER_PUNCTUATION);

    // symbols
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.MATH_SYMBOL);
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.CURRENCY_SYMBOL);
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.MODIFIER_SYMBOL);
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.OTHER_SYMBOL);

    // separators
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.SPACE_SEPARATOR);
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.LINE_SEPARATOR);
    CHAR_TYPES_ALL_BUT_CONTROL.add((int) Character.PARAGRAPH_SEPARATOR);
  }

  /**
   * Set of character types including only decimals, upper and lower case letters.
   */
  public static final Set<Integer> CHAR_TYPES_BASIC_ALPHA;

  static {
    CHAR_TYPES_BASIC_ALPHA = new HashSet<Integer>(2);
    CHAR_TYPES_BASIC_ALPHA.add((int) Character.UPPERCASE_LETTER);
    CHAR_TYPES_BASIC_ALPHA.add((int) Character.LOWERCASE_LETTER);
  }

  /**
   * Set of character types including only  decimals, upper and lower case letters.
   */
  public static final Set<Integer> CHAR_TYPES_BASIC_ALPHANUMERICS;

  static {
    CHAR_TYPES_BASIC_ALPHANUMERICS = new HashSet<Integer>(3);
    CHAR_TYPES_BASIC_ALPHANUMERICS.add((int) Character.DECIMAL_DIGIT_NUMBER);
    CHAR_TYPES_BASIC_ALPHANUMERICS.add((int) Character.UPPERCASE_LETTER);
    CHAR_TYPES_BASIC_ALPHANUMERICS.add((int) Character.LOWERCASE_LETTER);
  }

  /**
   * Set of character types including only decimals, letter numbers, 
   * other numbers, upper, lower, title case as well as letter modifiers 
   * and other letters.
   */
  public static final Set<Integer> CHAR_TYPE_EXTENDED_ALPHANUMERICS;

  static {
    CHAR_TYPE_EXTENDED_ALPHANUMERICS = new HashSet<Integer>(8);
    CHAR_TYPE_EXTENDED_ALPHANUMERICS.add((int) Character.DECIMAL_DIGIT_NUMBER);
    CHAR_TYPE_EXTENDED_ALPHANUMERICS.add((int) Character.LETTER_NUMBER);
    CHAR_TYPE_EXTENDED_ALPHANUMERICS.add((int) Character.OTHER_NUMBER);
    CHAR_TYPE_EXTENDED_ALPHANUMERICS.add((int) Character.UPPERCASE_LETTER);
    CHAR_TYPE_EXTENDED_ALPHANUMERICS.add((int) Character.LOWERCASE_LETTER);
    CHAR_TYPE_EXTENDED_ALPHANUMERICS.add((int) Character.TITLECASE_LETTER);
    CHAR_TYPE_EXTENDED_ALPHANUMERICS.add((int) Character.MODIFIER_LETTER);
    CHAR_TYPE_EXTENDED_ALPHANUMERICS.add((int) Character.OTHER_LETTER);
  }

  /** The character set to iterate over. */
  private final int[] characterSet;

  /** An array indices matching a position in the output string. */
  private int[] indices;

  /** The length of the output string in characters. */
  private final int length;

  /** The last value returned by the generator. Should be null if {@link #nextValue()}
   * has not been called.*/
  private String lastValue;

  /** Whether or not to throw an exception when the string rolls over. */
  private boolean throwExceptionOnRollover;

  /** Whether or not the generator has rolled over. */
  private boolean hasRolledOver;

  /**
   * Generates strings of 8 characters using only the upper and lower case alphabetical
   * characters from the ASCII set. 
   */
  public IncrementingPrintableStringGenerator() {
    this(DEFAULTSTRINGLENGTH, printableBasicAlphaASCIISet());
  }

  /**
   * Generates strings of {@link #length} characters using only the upper and lower 
   * case alphabetical characters from the ASCII set. 
   * @param length The length of string to return from the generator.
   * @throws IllegalArgumentException if the length is less than one.
   */
  public IncrementingPrintableStringGenerator(final int length) {
    this(length, printableBasicAlphaASCIISet());
  }

  /**
   * Generates strings of {@link #length} characters using the code points in
   * {@link #characterSet}.
   * @param length The length of string to return from the generator.
   * @param characterSet A set of code points to choose from. Code points in the 
   * set can be in any order, not necessarily lexical.
   * @throws IllegalArgumentException if the length is less than one or the character
   * set has fewer than one code points.
   */
  public IncrementingPrintableStringGenerator(final int length, final int[] characterSet) {
    if (length < 1) {
      throw new IllegalArgumentException("Length must be greater than or equal to 1");
    }
    if (characterSet == null || characterSet.length < 1) {
      throw new IllegalArgumentException("Character set must have at least one character");
    }
    this.length = length;
    this.characterSet = characterSet;
    indices = new int[length];
  }

  @Override
  public String nextValue() {
    if (hasRolledOver && throwExceptionOnRollover) {
      throw new NoSuchElementException("The generator has rolled over to the beginning");
    }

    final StringBuilder buffer = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      buffer.append(Character.toChars(characterSet[indices[i]]));
    }

    // increment the indices;
    for (int i = length - 1; i >= 0; --i) {
      if (indices[i] >= characterSet.length - 1) {
        indices[i] = 0;
        if (i == 0 || characterSet.length == 1 && lastValue != null) {
          hasRolledOver = true;
        }
      } else {
        ++indices[i];
        break;
      }
    }

    lastValue = buffer.toString();
    return lastValue;
  }

  @Override
  public String lastValue() {
    return lastValue;
  }

  /** @param exceptionOnRollover Whether or not to throw an exception on rollover. */
  public void setThrowExceptionOnRollover(final boolean exceptionOnRollover) {
    this.throwExceptionOnRollover = exceptionOnRollover;
  }

  /** @return Whether or not to throw an exception on rollover. */
  public boolean getThrowExceptionOnRollover() {
    return throwExceptionOnRollover;
  }

  /**
   * Returns an array of printable code points with only the upper and lower
   * case alphabetical characters from the basic ASCII set.
   * @return An array of code points
   */
  public static int[] printableBasicAlphaASCIISet() {
    final List<Integer> validCharacters =
        generatePrintableCharacterSet(0, 127, null, false, CHAR_TYPES_BASIC_ALPHA);
    final int[] characterSet = new int[validCharacters.size()];
    for (int i = 0; i < validCharacters.size(); i++) {
      characterSet[i] = validCharacters.get(i);
    }
    return characterSet;
  }

  /**
   * Returns an array of printable code points with the upper and lower case 
   * alphabetical characters as well as the numeric values from the basic 
   * ASCII set.
   * @return An array of code points
   */
  public static int[] printableBasicAlphaNumericASCIISet() {
    final List<Integer> validCharacters =
        generatePrintableCharacterSet(0, 127, null, false, CHAR_TYPES_BASIC_ALPHANUMERICS);
    final int[] characterSet = new int[validCharacters.size()];
    for (int i = 0; i < validCharacters.size(); i++) {
      characterSet[i] = validCharacters.get(i);
    }
    return characterSet;
  }

  /**
   * Returns an array of printable code points with the entire basic ASCII table,
   * including spaces. Excludes new lines.
   * @return An array of code points
   */
  public static int[] fullPrintableBasicASCIISet() {
    final List<Integer> validCharacters =
        generatePrintableCharacterSet(32, 127, null, false, null);
    final int[] characterSet = new int[validCharacters.size()];
    for (int i = 0; i < validCharacters.size(); i++) {
      characterSet[i] = validCharacters.get(i);
    }
    return characterSet;
  }

  /**
   * Returns an array of printable code points with the entire basic ASCII table,
   * including spaces and new lines.
   * @return An array of code points
   */
  public static int[] fullPrintableBasicASCIISetWithNewlines() {
    final List<Integer> validCharacters = new ArrayList<Integer>();
    validCharacters.add(10); // newline
    validCharacters.addAll(generatePrintableCharacterSet(32, 127, null, false, null));
    final int[] characterSet = new int[validCharacters.size()];
    for (int i = 0; i < validCharacters.size(); i++) {
      characterSet[i] = validCharacters.get(i);
    }
    return characterSet;
  }

  /**
   * Returns an array of printable code points the first plane of Unicode characters
   * including only the alpha-numeric values.
   * @return An array of code points
   */
  public static int[] printableAlphaNumericPlaneZeroSet() {
    final List<Integer> validCharacters =
        generatePrintableCharacterSet(0, 65535, null, false, CHAR_TYPES_BASIC_ALPHANUMERICS);
    final int[] characterSet = new int[validCharacters.size()];
    for (int i = 0; i < validCharacters.size(); i++) {
      characterSet[i] = validCharacters.get(i);
    }
    return characterSet;
  }

  /**
   * Returns an array of printable code points the first plane of Unicode characters
   * including all printable characters.
   * @return An array of code points
   */
  public static int[] fullPrintablePlaneZeroSet() {
    final List<Integer> validCharacters =
        generatePrintableCharacterSet(0, 65535, null, false, CHAR_TYPES_ALL_BUT_CONTROL);
    final int[] characterSet = new int[validCharacters.size()];
    for (int i = 0; i < validCharacters.size(); i++) {
      characterSet[i] = validCharacters.get(i);
    }
    return characterSet;
  }

  /**
   * Generates a list of code points based on a range and filters.
   * These can be used for generating strings with various ASCII and/or
   * Unicode printable character sets for use with DBs that may have 
   * character limitations.
   * <p>
   * Note that control, surrogate, format, private use and unassigned 
   * code points are skipped.
   * @param startCodePoint The starting code point, inclusive.
   * @param lastCodePoint The final code point, inclusive.
   * @param characterTypesFilter An optional set of allowable character
   * types. See {@link Character} for types.
   * @param isFilterAllowableList Determines whether the {@code allowableTypes}
   * set is inclusive or exclusive. When true, only those code points that
   * appear in the list will be included in the resulting set. Otherwise
   * matching code points are excluded.
   * @param allowableTypes An optional list of code points for inclusion or
   * exclusion.
   * @return A list of code points matching the given range and filters. The
   * list may be empty but is guaranteed not to be null.
   */
  public static List<Integer> generatePrintableCharacterSet(
      final int startCodePoint,
      final int lastCodePoint,
      final Set<Integer> characterTypesFilter,
      final boolean isFilterAllowableList,
      final Set<Integer> allowableTypes) {

    // since we don't know the final size of the allowable character list we
    // start with a list then we'll flatten it to an array.
    final List<Integer> validCharacters = new ArrayList<Integer>(lastCodePoint);

    for (int codePoint = startCodePoint; codePoint <= lastCodePoint; ++codePoint) {
      if (allowableTypes != null &&
          !allowableTypes.contains(Character.getType(codePoint))) {
        continue;
      } else {
        // skip control points, formats, surrogates, etc
        final int type = Character.getType(codePoint);
        if (type == Character.CONTROL ||
            type == Character.SURROGATE ||
            type == Character.FORMAT ||
            type == Character.PRIVATE_USE ||
            type == Character.UNASSIGNED) {
          continue;
        }
      }

      if (characterTypesFilter != null) {
        // if the filter is enabled then we need to make sure the code point 
        // is in the allowable list if it's a whitelist or that the code point
        // is NOT in the list if it's a blacklist.
        if ((isFilterAllowableList && !characterTypesFilter.contains(codePoint)) ||
            (characterTypesFilter.contains(codePoint))) {
          continue;
        }
      }

      validCharacters.add(codePoint);
    }
    return validCharacters;
  }

}
