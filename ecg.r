library("ggplot2")
# install.packages("dplyr")
library("dplyr")
#install.packages("reshape")
library("reshape")

df <-
  read.table("data/100.csv",
             col.names=c("ts", "ch0", "ch1"),
             skip=0, header=FALSE, sep=",")
head(df)
tail(df)

# Adjust measurements
df <- df %>% mutate(ch0=ch0-1024, ch1=ch1-1024)
head(df)
tail(df)

# Smoothing (window size = 3)
for (i in 1:nrow(df)) {
  sum = 0.0;
  sum = if (i - 1 >= 1) { sum + df[i - 1, "ch0"] } else { sum };
  sum = sum + df[i, "ch0"];
  sum = if (i + 1 <= nrow(df)) { sum + df[i + 1, "ch0"] } else { sum };
  df[i, "smooth"] = sum / 3;
}
head(df)
tail(df)

# Smoothing (window size = 5)
for (i in 1:nrow(df)) {
  sum = 0.0;
  sum = if (i - 2 >= 1) { sum + df[i - 2, "ch0"] } else { sum };
  sum = if (i - 1 >= 1) { sum + df[i - 1, "ch0"] } else { sum };
  sum = sum + df[i, "ch0"];
  sum = if (i + 1 <= nrow(df)) { sum + df[i + 1, "ch0"] } else { sum };
  sum = if (i + 2 <= nrow(df)) { sum + df[i + 2, "ch0"] } else { sum };
  df[i, "smooth"] = sum / 5;
}
head(df)
tail(df)

# Derivative
df[1, "deriv"] = df[2, "smooth"] / 2;
for (i in 2:nrow(df)) {
  if (i < nrow(df)) {
    df[i, "deriv"] =
      (df[i + 1, "smooth"] - df[i - 1, "smooth"]) / 2;
  } else {
    df[i, "deriv"] = - df[i - 1, "smooth"] / 2;
  }
}
head(df)
tail(df)

# Tmp
df <- df %>% mutate(tmp=sqrt(1+deriv*deriv))
head(df)
tail(df)

# Calculate length
for (i in 1:nrow(df)) {
  sum = df[i, "tmp"];
  for (j in 1:10) {
    if (i + j <= nrow(df)) {
      sum = sum + df[i + j, "tmp"];
    }
    if (i - j >= 1) {
      sum = sum + df[i - j, "tmp"];
    } 
  }
  df[i, "length"] = sum;
}
head(df)
tail(df)

# average length
df_length_avg <- df %>%
  summarize(avg=mean(length))
df_length_avg

df <- merge(df, df %>% summarize(avg_length=mean(length)))
head(df)
tail(df)

# restructure the table
df_var <-
  melt(data=df, id.vars=c("ts"),
       measure.vars = c("ch0", "smooth", "deriv", "length", "avg_length"))
head(df_var)
tail(df_var)

# original, smooth, deriv, length, average length
df_plot <- df_var %>% filter(ts >= 3000 & ts <= 4000)
ggplot(df_plot, aes(x=ts, y=value, color=variable)) +
  geom_line(size=1)

# original
df_plot <- df %>% filter(ts < 5000)
ggplot(df_plot, aes(x=ts, y=ch0)) +
  #geom_point() +
  geom_line()

# smooth
df_plot <- df %>% filter(ts < 500)
ggplot(df_plot, aes(x=ts, y=smooth)) +
  #geom_point() +
  geom_line()

# deriv
df_plot <- df %>% filter(ts < 500)
ggplot(df_plot, aes(x=ts, y=deriv)) +
  geom_line()

# length
df_plot <- df %>% filter(ts < 500)
ggplot(df_plot, aes(x=ts, y=length)) +
  geom_line()

ggsave("ECG.pdf", width=16, height=9)
ggsave("ECG.pdf", width=40, height=9)

