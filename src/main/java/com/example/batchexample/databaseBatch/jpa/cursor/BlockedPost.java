package com.example.batchexample.databaseBatch.jpa.cursor;

import jakarta.persistence.*;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;


@Entity
@Table(name = "blocked_posts")
@Getter
@NoArgsConstructor
public class BlockedPost {
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "blocked_posts_id_seq")
    @SequenceGenerator(name = "blocked_posts_id_seq", sequenceName = "blocked_posts_id_seq")
    private Long id;

    @Column(name = "post_id")
    private Long postId;

    private String writer;
    private String title;

    @Column(name = "report_count")
    private int reportCount;

    @Column(name = "block_score")
    private double blockScore;

    @Column(name = "blocked_at")
    private LocalDateTime blockedAt;

    @Builder
    public BlockedPost(Long postId, String writer, String title,
                       int reportCount, double blockScore, LocalDateTime blockedAt) {
        this.postId = postId;
        this.writer = writer;
        this.title = title;
        this.reportCount = reportCount;
        this.blockScore = blockScore;
        this.blockedAt = blockedAt;
    }
}